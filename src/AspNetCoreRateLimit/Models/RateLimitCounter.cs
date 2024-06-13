using System;

namespace AspNetCoreRateLimit
{
    /// <summary>
    /// Stores the initial access time and the numbers of calls made from that point
    /// </summary>
    public struct RateLimitCounter
    {
        public DateTime Timestamp { get; set; }

        public double Count { get; set; }

        public bool IsBlocked { get; set; }

        private DateTime? _blockedAt { get; set; }
        
        public DateTime BlockedAt
        {
            get => _blockedAt ??= Timestamp;
            set => _blockedAt = value;
        }
    }
}