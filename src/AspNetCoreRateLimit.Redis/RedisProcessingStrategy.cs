using AspNetCoreRateLimit;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace AspNetCoreRateLimit.Redis
{
    public class RedisProcessingStrategy : ProcessingStrategy
    {
        private readonly IConnectionMultiplexer _connectionMultiplexer;
        private readonly IRateLimitConfiguration _config;
        private readonly ILogger<RedisProcessingStrategy> _logger;

        public RedisProcessingStrategy(IConnectionMultiplexer connectionMultiplexer, IRateLimitConfiguration config, ILogger<RedisProcessingStrategy> logger)
            : base(config)
        {
            _connectionMultiplexer = connectionMultiplexer ?? throw new ArgumentException("IConnectionMultiplexer was null. Ensure StackExchange.Redis was successfully registered");
            _config = config;
            _logger = logger;
        }

        static private readonly LuaScript _atomicIncrement = LuaScript.Prepare("local count = redis.call(\"INCRBYFLOAT\", @key, tonumber(@delta)) local ttl = redis.call(\"TTL\", @key) if ttl == -1 then redis.call(\"EXPIRE\", @key, @timeout) end return count");

        public override async Task<RateLimitCounter> ProcessRequestAsync(ClientRequestIdentity requestIdentity, RateLimitRule rule, ICounterKeyBuilder counterKeyBuilder, RateLimitOptions rateLimitOptions, CancellationToken cancellationToken = default)
        {
            var counterId = BuildCounterKey(requestIdentity, rule, counterKeyBuilder, rateLimitOptions);
            var limitCounter = await IncrementAsync(counterId, rule.PeriodTimespan.Value, rule, _config.RateIncrementer);
            
            return limitCounter;
        }

        private async Task<RateLimitCounter> IncrementAsync(string counterId, TimeSpan interval, RateLimitRule rule, Func<double> RateIncrementer = null)
        {
            var now = DateTime.UtcNow;
            var numberOfIntervals = now.Ticks / interval.Ticks;
            var intervalStart = new DateTime(numberOfIntervals * interval.Ticks, DateTimeKind.Utc);

            _logger.LogDebug("Calling Lua script. {counterId}, {timeout}, {delta}", counterId, interval.TotalSeconds, 1D);
            var db = _connectionMultiplexer.GetDatabase();
            var count = await db.ScriptEvaluateAsync(_atomicIncrement, new { key = new RedisKey(counterId), timeout = interval.TotalSeconds, delta = RateIncrementer?.Invoke() ?? 1D });
            var block = await CheckIfBlockedAsync(counterId, (double)count, rule, intervalStart);
            
            var limitCounter = new RateLimitCounter
            {
                Count = (double)count,
                Timestamp = intervalStart,
            };
            
            if (block.isBlocked)
            {
                limitCounter.IsBlocked = true;
                limitCounter.BlockedAt = block.blockedAt;
            }
            
            return limitCounter;
        }
        
        private async Task<(bool isBlocked, DateTime blockedAt)> CheckIfBlockedAsync(string counterId, double count, RateLimitRule rule, DateTime intervalStart)
        {
            var db = _connectionMultiplexer.GetDatabase();
            var key = new RedisKey($"IsBlocked-{counterId}");
            var isBlockedRedis = await db.HashGetAsync(key, "IsBlocked");
            var blockedAtRedis = await db.HashGetAsync(key, "BlockedAt");

            if (isBlockedRedis.HasValue && blockedAtRedis.HasValue)
            {
                var isBlocked = (bool)isBlockedRedis;
                var blockedAt = new DateTime((long)blockedAtRedis, DateTimeKind.Utc);
                return (isBlocked, blockedAt);
            }

            if (intervalStart + rule.PeriodTimespan.Value >= DateTime.UtcNow)
            {
                var isBlocked = count > rule.Limit;
                var now = DateTime.UtcNow;

                if (isBlocked)
                {
                    await db.HashSetAsync(key, new[] { new HashEntry("IsBlocked", true), new HashEntry("BlockedAt", now.Ticks) });
                    await db.KeyExpireAsync(key, rule.BlockPeriodTimespan.Value);
                }
                
                return (isBlocked, now);
            }

            return (false, DateTime.MinValue);
        }
    }
}
