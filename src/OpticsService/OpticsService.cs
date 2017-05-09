using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using PiJobs.Shared.Contracts;
using PiJobs.Shared.Optics;

using OpticsCollection =
    Microsoft.ServiceFabric.Data.Collections
    .IReliableDictionary<string, PiJobs.Shared.Optics.OpticsEvent>;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using System.Globalization;
using System.Collections.Concurrent;

namespace OpticsService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class OpticsService : StatefulService, IOpticsService
    {
        OpticsSettings _settings;
        ConcurrentDictionary<string, ConcurrentDictionary<string, OpticsEvent>> 
            _cachedOptics = new ConcurrentDictionary<string, ConcurrentDictionary<string, OpticsEvent>>();
        public OpticsService(StatefulServiceContext context)
            : base(context)
        {

            _settings = 
                new OpticsSettings(
                    context.CodePackageActivationContext
                    .GetConfigurationPackageObject("Config"));

        }

        public async Task Add(OpticsEvent @event)
        {
            long eventTicks = DateTime.UtcNow.Ticks;
            string eventGuid = Guid.NewGuid().ToString();
            string eventKey = $"{eventTicks}:{eventGuid}";
            string bucketKey = GenerateBucketKey(@event);

            var bucket = await StateManager.GetOrAddAsync<OpticsCollection>(bucketKey);

            //we cache in local memory just to optimize around RC snapshots for a lot of queries.
            //only for demo.
            ConcurrentDictionary<string, OpticsEvent> cacheBucket = null;
            if (!_cachedOptics.TryGetValue(bucketKey, out cacheBucket))
            {
                _cachedOptics[bucketKey] = cacheBucket = new ConcurrentDictionary<string, OpticsEvent>();
            }
            cacheBucket[eventKey] = @event;

            using (var tx = StateManager.CreateTransaction())
            {
                await bucket.SetAsync(tx, eventKey, @event);
                await tx.CommitAsync();
            }
        }

        public async Task<QueryResults<OpticsEvent>> Query(DateTime start, DateTime end, List<KeyValuePair<string, string>> query, long ContinueToken)
        {
            //fast forward start to continue token if set.
            if (ContinueToken != 0)
            {
                start = new DateTime(ContinueToken);
            }

            var allBuckets = await GetBucketList();
            var buckets = allBuckets.Where(e =>
            {
                var dt = DateTime.ParseExact(e, _settings.Grain, CultureInfo.InvariantCulture);
                var startBucket = DateTime.ParseExact(start.ToString(_settings.Grain), _settings.Grain, CultureInfo.InvariantCulture);
                var endBucket = DateTime.ParseExact(end.ToString(_settings.Grain), _settings.Grain, CultureInfo.InvariantCulture);
                return (dt >= startBucket && dt <= endBucket);
            });

            QueryResults<OpticsEvent> results = new QueryResults<OpticsEvent>()
            {
                Items = new List<OpticsEvent>()
            };
            foreach (var bucket in buckets)
            {
                ConcurrentDictionary<string, OpticsEvent> cacheBucket = null;
                if (!_cachedOptics.TryGetValue(bucket, out cacheBucket))
                {
                    _cachedOptics[bucket] = cacheBucket = new ConcurrentDictionary<string, OpticsEvent>();
                }

                List<Tuple<long, OpticsEvent>> optics = new List<Tuple<long, OpticsEvent>>();
                foreach (var kvp in cacheBucket)
                {
                    var eventKey = kvp.Key;
                    var @event = kvp.Value;

                    var parts = eventKey.Split(':');
                    var ticks = long.Parse(parts[0]);
                    if (ContinueToken != 0 && ticks < ContinueToken)
                    {
                        continue;
                    }

                    optics.Add(new Tuple<long, OpticsEvent>(ticks, @event));
                }

                optics = optics.OrderBy(e => e.Item1).ToList();
                foreach(var optic in optics)
                {
                    var ticks = optic.Item1;
                    var @event = optic.Item2;
                    if (@event.Timestamp >= start
                                               && @event.Timestamp <= end
                                               && QueryPass(query, @event))
                    {
                        if (results.Items.Count > _settings.QuerySize)
                        {
                            results.ContinueToken = ticks;
                            return results;
                        }
                        results.Items.Add(@event);
                    }
                } 
            }
            return results;
        }

        private bool QueryPass(List<KeyValuePair<string,string>> query, OpticsEvent @event)
        {

            foreach(var kvp in query)
            {
                var key = kvp.Key;
                var @value = kvp.Value;

                if (@event.Properties.ContainsKey(key))
                {
                    if (!(@event.Properties[key] == @value))
                        return false;
                }
                else
                {
                    return false;
                }
            }

            return true;
        }

        private async Task<List<string>> GetBucketList()
        {
            List<string> buckets = new List<string>();
            var iter = StateManager.GetAsyncEnumerator();
            while(await iter.MoveNextAsync(CancellationToken.None))
            {
                var rc = iter.Current;
                buckets.Add(rc.Name.ToString().Replace("urn:",""));
            }
            return buckets;

        }

       
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]{
                new ServiceReplicaListener(
                    (context) 
                    => new FabricTransportServiceRemotingListener(context,this))};
        }

        //since we cache optics in local memory, RunAsync is used to reload in case it's a fail recovery
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {

            var rcIter = StateManager.GetAsyncEnumerator();
            while(await rcIter.MoveNextAsync(cancellationToken))
            {
                var rc = rcIter.Current;
                var oc = rc as OpticsCollection;
                if (oc != null)
                {
                    var bucketName = rc.Name.ToString().Replace("urn:", "");
                    ConcurrentDictionary<string, OpticsEvent> cacheBucket = null;
                    if (!_cachedOptics.TryGetValue(bucketName, out cacheBucket))
                    {
                        _cachedOptics[bucketName] = cacheBucket = new ConcurrentDictionary<string, OpticsEvent>();
                    }
                    using (var tx = StateManager.CreateTransaction())
                    {
                        var itr = (await oc.CreateEnumerableAsync(tx)).GetAsyncEnumerator();
                        while(await itr.MoveNextAsync(cancellationToken))
                        {
                            cacheBucket[itr.Current.Key] = itr.Current.Value;
                        }
                    }
                }
            }           
        }

        private string GenerateBucketKey(OpticsEvent @event)
        {
            return @event.Timestamp.ToString(_settings.Grain);           
        }

    }
}
