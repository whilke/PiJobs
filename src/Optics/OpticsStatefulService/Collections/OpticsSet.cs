using Microsoft.ServiceFabric.Data;
using PiJobs.Shared.Optics;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PiOptics
{

    static class OpticOptions
    {
        public static bool Locked { get; set; }
    }
    /// <summary>
    /// High level collection of optics based around a time span key.
    /// Current span is at the hour level which will group all optics within the same hour to the same collection
    /// Optics are dropped by removing the entire key collection at once. This is set to a 24h period.
    /// </summary>
    public class OpticsSet
    {
        private const string KEY_FORMAT = "yyyyMMddHH";
        IReliableStateManager _manager;
        OpticsService _optics;

        ConcurrentDictionary<string, OpticBucket> _buckets = new
            ConcurrentDictionary<string, OpticBucket>();

        
        public OpticsSet(IReliableStateManager manager, OpticsService optics)
        {
            _optics = optics;
            _manager = manager;

            //see if we have any failed over event data to rebuild.
            //this happens if a process recovers from a crash or if a secondary replica promotes to primary
            //while secondary has the RD information the indexed lookups need to be rebuilt.
            CancellationToken ct = new CancellationToken();
            var er = _manager.GetAsyncEnumerator();
            while(er.MoveNextAsync(ct).Result)
            {
                var state = er.Current;
                var name = state.Name.ToString();
                if (name.Contains("optics_"))
                {
                    var parts = name.Split('_');
                    if (parts.Length > 1)
                    {
                        var key = parts[1];
                        if (key.Contains(":o")) continue;

                        var bucket = new OpticBucket(key, _manager, _optics);
                        _buckets.TryAdd(key, bucket);

                        if (OpticOptions.Locked)
                            bucket.Lock().GetAwaiter().GetResult();
                    }
                }
            }
        }

        public async Task Lock()
        {
            OpticOptions.Locked = true;
            foreach(var b in _buckets)
            {
                await b.Value.Lock();
            }
        }

        public async Task Unlock()
        {
            OpticOptions.Locked = false;

            foreach (var b in _buckets)
            {
                await b.Value.Unlock();
            }
        }

        public async Task Purge()
        {
            var keys = _buckets.Keys;
            foreach (var key in keys)
            {
                OpticBucket b;
                if (_buckets.TryRemove(key, out b))
                {
                    await b.Drop(true);
                }
            }
        }

        public async Task AddRange(List<OpticsEvent> evt)
        {
            Dictionary<string, List<OpticsEvent>> bucketsplit = new Dictionary<string, List<OpticsEvent>>();
            foreach(var e in evt)
            {
                var key = GetKey(e.Timestamp);
                List<OpticsEvent> optics;
                if (!bucketsplit.TryGetValue(key, out optics))
                {
                    bucketsplit[key] = optics = new List<OpticsEvent>();
                }
                optics.Add(e);
            }

            foreach(var kvp in bucketsplit)
            {
                OpticBucket bucket = null;
                var key = kvp.Key;
                if (!_buckets.TryGetValue(key, out bucket))
                {
                    bucket = new OpticBucket(key, _manager, _optics);
                    bucket = _buckets.AddOrUpdate(key, bucket, (k, b) =>
                    {
                        //might need to merge old bucket (b) into new bucket.
                        return bucket;
                    });                    
                }

                await bucket.AddRange(kvp.Value);
            }
        }

        public Task Add(OpticsEvent evt)
        {
            //figure out which key bucket the event lives in.
            var key = GetKey(evt.Timestamp);

            //if this event is for a bucket not already created, we need to first create it.
            OpticBucket bucket = null;
            if (!_buckets.TryGetValue(key, out bucket))
            {
                bucket = new OpticBucket(key, _manager, _optics);
                bucket = _buckets.AddOrUpdate(key, bucket, (k, b) =>
                {
                    //might need to merge old bucket (b) into new bucket.
                    return bucket;
                });
            }

            return bucket.Add(evt);
        }

        public static int GetSecondsSinceEpoch(DateTime dt)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            TimeSpan diff = dt - origin;
            return (int)diff.TotalSeconds;
        }

        public static DateTime GetDTFromEpochCounter(int seconds)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            return origin.AddSeconds(seconds);
        }

        public static long ToLong(int left, int right)
        {
            long v = (long)left << 32 | (long)(uint)right;
            return v;
        }

        public static int[] FromLong(long a)
        {
            int a1 = (int)(a & uint.MaxValue);
            int a2 = (int)(a >> 32);
            return new int[] { a1, a2 };
        }

        public async Task<List<OpticsEvent>> Query(string query, DateTime startDt, DateTime endDT, int maxCount, long continueToken)
        {
            maxCount++;

            //clip start/end input times to within the bounds of the buckets this set knows about.
            var startDtRound = RoundToBucket(startDt);
            var endDTRound = RoundToBucket(endDT);
            var parts = FromLong(continueToken);
            var part_dt = parts[0];
            var part_ct = parts[1];
            //get list of buckets to search first.
            List<OpticBucket> buckets = new List<OpticBucket>();
            foreach (var key in _buckets.Keys)
            {
                var dt = ConvertFromKey(key);
                if (dt >= startDtRound && dt <= endDTRound)
                {
                    if (continueToken != 0)
                    {
                       
                        var pDt = GetDTFromEpochCounter(part_dt);
                        if (dt >= pDt)
                        {
                            buckets.Add(_buckets[key]);
                        }
                    }
                    else
                    {
                        buckets.Add(_buckets[key]);
                    }
                }
            }

            //sort the buckets now
            buckets = buckets.OrderBy(e => ConvertFromKey(e.Key)).ToList();

            //for each bucket run the search across it's data set.
            //in the future this might become a mult-task operation assuming the lock on the RD transaction doesn't block us.
            //maybe one transaction for all sub-searches.
            List<OpticsEvent> ets = new List<OpticsEvent>();
            int runningCount = maxCount;
            foreach(var bucket in buckets)
            {
                var dt = ConvertFromKey(bucket.Key);
                var first_part = GetSecondsSinceEpoch(dt);
                var ctoken = 0;
                //if the part_ct was from a different token we want to reset to 0 since we've searched across a bucket now.
                if (continueToken != 0 && first_part == part_dt)
                {
                    ctoken = part_ct;
                }

                var e = await bucket.Search(query, runningCount, ctoken, startDt, endDT);
                e = e.Select(e1 => { e1.bucketId = first_part; return e1; }).ToList();
                ets.AddRange(e);

                runningCount -= e.Count;
                if (runningCount <= 0) break;
            }
          
            return ets;

        }

        public async Task<int> QueryCount(string query, DateTime startDt, DateTime endDT)
        {
            //clip start/end input times to within the bounds of the buckets this set knows about.
            var startDtRound = RoundToBucket(startDt);
            var endDTRound = RoundToBucket(endDT);

            //get list of buckets to search first.
            List<OpticBucket> buckets = new List<OpticBucket>();
            foreach (var key in _buckets.Keys)
            {
                var dt = ConvertFromKey(key);
                if (dt >= startDtRound && dt <= endDTRound)
                {
                    buckets.Add(_buckets[key]);
                }
            }

            //for each bucket run the search across it's data set.
            //in the future this might become a mult-task operation assuming the lock on the RD transaction doesn't block us.
            //maybe one transaction for all sub-searches.
            int count = 0;
            foreach (var bucket in buckets)
            {
                var e = await bucket.Search(query, 0, 0, startDt, endDT);
                count += e.Count;
            }

            return count;

        }

        private string GetKey(DateTime dt)
        {
            return dt.ToString("yyyyMMddHH");
        }

        private DateTime RoundToBucket(DateTime dt)
        {
            var key = GetKey(dt);
            return ConvertFromKey(key);           
        }

        private DateTime ConvertFromKey(string key)
        {
            return DateTime.ParseExact(key, KEY_FORMAT, CultureInfo.InvariantCulture);
        }

        //removes entire buckets that are older then 25 hours.
        public async Task Dropdata()
        {
            var minDate = DateTime.UtcNow.Subtract(TimeSpan.FromHours(25));

            var keys = _buckets.Keys;
            foreach(var key in keys)
            {
                var dt = ConvertFromKey(key);

                if (dt < minDate)
                {
                    OpticBucket b;
                    if (_buckets.TryRemove(key, out b))
                    {
                        await b.Drop();
                    }
                }
            }
        }
    }
}
