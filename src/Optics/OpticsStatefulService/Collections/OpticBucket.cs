using PiOptics.Ast;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;

using OpticCollectionType = Microsoft.ServiceFabric.Data.Collections.IReliableDictionary<int, PiJobs.Shared.Optics.OpticsEvent>;
using OpticCollectionOptionsType = Microsoft.ServiceFabric.Data.Collections.IReliableDictionary<string, string>;
using System.Threading;
using System;
using System.Linq;
using System.Runtime.Caching;
using PiJobs.Shared.Optics;

namespace PiOptics
{
    /// <summary>
    /// Wrapper around a reliable dictionary to store append only optic events.
    /// We use this wrapper as we also build in-memory lookups against all properties for fast queries.
    /// </summary>
    public class OpticBucket
    {
        string _key;
        IReliableStateManager _stateManager;
        OpticsService _optics;
        object _lockobject = new object();
        OpticCollectionType _collection;
        OpticCollectionOptionsType _options;

        BitmapIndex.BitmapIndex _index = new BitmapIndex.BitmapIndex();
        object sync = new object();
        private int logId = -1;

        public string Key { get; set; }
        internal OpticBucket(string key, IReliableStateManager stateManager, OpticsService optics)
        {
            Key = key;
            _optics = optics;
            _key = "optics_" + key;
            _stateManager = stateManager;
            _collection = _stateManager.GetOrAddAsync<OpticCollectionType>(_key).GetAwaiter().GetResult();
            _options = _stateManager.GetOrAddAsync<OpticCollectionOptionsType>(_key + ":o").GetAwaiter().GetResult();

            //rebuild bitmap indexes if needed
            rebuild();
        }

        //rebuild the in-memory lookup indexes in case there is data already in the RD when this starts up.
        //otherwise lookups are updated as inserts happen
        private void rebuild()
        {
            lock (sync)
            {
                _index = new BitmapIndex.BitmapIndex();
                CancellationToken ct = new CancellationToken();
                List<OpticsEvent> events = new List<OpticsEvent>();
                using (var tx = _stateManager.CreateTransaction())
                {
                    var count = _collection.GetCountAsync(tx).GetAwaiter().GetResult();
                    if (count > 0)
                    {
                        var e = _collection.CreateEnumerableAsync(tx).Result;
                        var er = e.GetAsyncEnumerator();
                        while (er.MoveNextAsync(ct).Result)
                        {
                            var evt = er.Current.Value;
                            events.Add(evt);
                        }
                    }
                }

                if (events.Count == 0) return;
                var orderedEvents = events.OrderBy(evt => evt.Id);
                logId = orderedEvents.Last().Id;
                foreach (var evt in orderedEvents)
                {
                    foreach (var kvp in evt.Properties)
                    {
                        BitmapIndex.BIKey key = new BitmapIndex.BIKey(kvp.Key.GetHashCode(), kvp.Value);
                        _index.Set(key, evt.Id);
                    }
                }
            }

        }

        public async Task Lock()
        {
            if (_options != null && _stateManager != null)
            {
                using (var tx = _stateManager.CreateTransaction())
                {
                    await _options.SetAsync(tx, "skip_delete", "true");
                    await tx.CommitAsync();
                }
            }
        }

        public async Task Unlock()
        {
            if (_options != null && _stateManager != null)
            {
                using (var tx = _stateManager.CreateTransaction())
                {
                    await _options.TryRemoveAsync(tx, "skip_delete");
                    await tx.CommitAsync();
                }
            }
        }

        public Task Add(OpticsEvent evt)
        {
            return Task.Run(() =>
             {
                 using (var tx = _stateManager.CreateTransaction())
                 {
                     lock (sync)
                     {

                        //we are assuming the transaction is a lock for at least this partition
                        //we only need id's to be unique per partition.

                        int id = ++logId;

                         evt.Id = id;
                         _collection.AddAsync(tx, id, evt).GetAwaiter().GetResult();

                        //update in-memory lookup indexes.
                        foreach (var kvp in evt.Properties)
                         {
                             BitmapIndex.BIKey key = new BitmapIndex.BIKey(kvp.Key.GetHashCode(), kvp.Value);
                             _index.Set(key, evt.Id);
                         }
                         tx.CommitAsync().GetAwaiter().GetResult();
                     }
                 }
             });
        }

        public Task AddRange(List<OpticsEvent> events)
        {
            return Task.Run(() =>
             {
                 using (var tx = _stateManager.CreateTransaction())
                 {
                     lock (sync)
                     {
                         foreach (var evt in events)
                         {
                            //we are assuming the transaction is a lock for at least this partition
                            //we only need id's to be unique per partition.

                            int id = ++logId;

                             evt.Id = id;
                             _collection.AddAsync(tx, id, evt).GetAwaiter().GetResult();

                            //update in-memory lookup indexes.
                            foreach (var kvp in evt.Properties)
                             {
                                 BitmapIndex.BIKey key = new BitmapIndex.BIKey(kvp.Key.GetHashCode(), kvp.Value);
                                 _index.Set(key, evt.Id);
                             }
                         }
                         tx.CommitAsync().GetAwaiter().GetResult();
                     }
                 }
             });
        }

        public async Task Drop(bool rebuild = false)
        {
            if (_options != null && _stateManager != null)
            {
                using (var tx = _stateManager.CreateTransaction())
                {
                    var c = await _options.TryGetValueAsync(tx, "skip_delete");
                    if (c.HasValue && bool.Parse(c.Value))
                        return;
                }
            }

            if (_collection != null && _stateManager != null)
            {
                await _stateManager.RemoveAsync(_key);
                await _stateManager.RemoveAsync(_key+":o");
                if (rebuild)
                {
                    _collection = await _stateManager.GetOrAddAsync<OpticCollectionType>(_key);
                    _options = await _stateManager.GetOrAddAsync<OpticCollectionOptionsType>(_key + ":o");
                }
            }
        }

        private BitmapIndex.BICriteria BuildQuery(string query)
        {
            var cache = MemoryCache.Default;
            if ( cache.Contains(query) )
            {
                return cache[query] as BitmapIndex.BICriteria;
            }
            else
            {
                BitmapIndex.BICriteria search = null;

                //from the query build a grammer tree of the expressions that we can walk.
                var g = Grammer.ParseExpression(query);

                //the walker here will allow us to build up an ordered stack of operations based on grammer types.
                //once we have a stack of operations we can loop through stack and apply the ops.
                //the stack is purely to support the params sub grouping order
                AstWalker walker = new AstWalker();

                bool isNewGroup = false;
                Stack<Operator> opStack = new Stack<Operator>();
                Stack<Operator> opGroupStack = new Stack<Operator>();
                walker.Opcallback = (op) =>
                {
                    if (isNewGroup)
                    {
                        opGroupStack.Push(op);
                        isNewGroup = false;
                    }
                    else
                        opStack.Push(op);
                };

                Stack<BitmapIndex.BICriteria> searchStack = new Stack<BitmapIndex.BICriteria>();
                Stack<BitmapIndex.BICriteria> groupStack = new Stack<BitmapIndex.BICriteria>();
                walker.AssignCallback = (l, r) =>
                {
                    var aop = BitmapIndex.BICriteria.equals(new BitmapIndex.BIKey(l.GetHashCode(), r));
                    searchStack.Push(aop);

                    if (searchStack.Count > 1)
                    {
                        var op1 = searchStack.Pop();
                        var op2 = searchStack.Pop();
                        var op = opStack.Pop();

                        if (!isNewGroup)
                        {
                            if (op == Operator.And)
                                op1 = op2.andEquals(op1.Key);
                            else
                                op1 = op2.orEquals(op1.Key);

                            search = op1;
                        }
                        else
                        {
                            isNewGroup = false;

                            if (op == Operator.And)
                                search = search.and(aop);
                            else
                                search = search.or(aop);
                        }

                        searchStack.Push(search);
                    }
                };

                walker.NotAssignCallback = (l, r) =>
                {
                    var aop = BitmapIndex.BICriteria.notEquals(new BitmapIndex.BIKey(l.GetHashCode(), r));
                    searchStack.Push(aop);

                    if (searchStack.Count > 1)
                    {
                        var op1 = searchStack.Pop();
                        var op2 = searchStack.Pop();
                        var op = opStack.Pop();

                        if (!isNewGroup)
                        {
                            if (op == Operator.And)
                                op1 = op2.andNotEquals(op1.Key);
                            else
                                op1 = op2.orNotEquals(op1.Key);

                            search = op1;
                        }
                        else
                        {
                            isNewGroup = false;

                            if (op == Operator.And)
                                search = search.and(aop);
                            else
                                search = search.or(aop);
                        }

                        searchStack.Push(search);
                    }
                };

                walker.StartGroupCallback = () =>
                {
                    var op1 = searchStack.Pop();
                    groupStack.Push(op1);
                };

                walker.Walk(g);

                groupStack.Push(searchStack.Pop());

                //each sub group within the params is already setup.
                //go through each params group and apply correct and/or operator to bitmap index.
                search = null;
                while (groupStack.Count > 0)
                {
                    var op = groupStack.Pop();
                    if (groupStack.Count == 0)
                    {
                        if (search == null)
                        {
                            search = op;
                        }
                        else
                        {
                            var opV = opStack.Pop();
                            if (opV == Operator.And)
                                search = search.and(op);
                            else if (opV == Operator.Or)
                                search = search.or(op);
                        }
                    }
                    else
                    {
                        var op2 = groupStack.Pop();
                        if (search == null)
                        {
                            search = op;
                        }

                        var opV = opStack.Pop();
                        if (opV == Operator.And)
                            search = search.and(op2);
                        else if (opV == Operator.Or)
                            search = search.or(op2);
                    }
                }

                cache.Add(query, search, DateTimeOffset.UtcNow.AddMinutes(10));
                return search;
            }
        }

        public async Task<List<OpticsEvent>> Search(string query, int maxCount, int startId, DateTime startDt, DateTime endDt)
        {
            BitmapIndex.BICriteria search = null;
            if (!string.IsNullOrEmpty(query))
            {
                search = BuildQuery(query);
            }
            else
            {
                search = BitmapIndex.BICriteria.notEquals(new BitmapIndex.BIKey(0,""));
            }


            Ewah.EwahCompressedBitArray ret_array = null;
            if (search != null)
            {
                ret_array = _index.query(search);
            }

            List<OpticsEvent> retVals = new List<OpticsEvent>();
            {
                using (var tx = _stateManager.CreateTransaction())
                {
                    if (ret_array == null)
                    {
                        //this should never happen anymore with the alt query that returns all results.
                    }
                    else
                    {
                        //the bitmap query just returns us back the ids that match.
                        //those id's are the keys into the RD, which means we can do quick hash lookups to get the real data.
                        var ids = ret_array.GetPositions();
                        int count = 0;
                        ids = ids.OrderBy(i => i).ToList();

                        if (startId != 0)
                        {
                            ids = ids.Where(e => e > startId).ToList();
                        }

                        if (ids.Count > 0)
                        {
                            //run a divide and conquer against the list to quickly get to the right starting id
                            //log streaming will often just try and grab logs in the middle of the list over and over.

                            int idx = ids.Count / 2;
                            int low = 0;
                            int high = ids.Count;
                            Func<Task<int>> conq = null;
                            conq = new Func<Task<int>>(async () =>
                            {
                                var size = (high - low);
                                if (size < 20) return low;

                                var id = ids[idx];
                                var r = await _collection.TryGetValueAsync(tx, id);
                                if (r.HasValue)
                                {
                                    var ev = r.Value;
                                    var dt = ev.Timestamp;

                                    if (dt < startDt)
                                    {
                                        //not far enough, jump forward and check again
                                        low = idx;
                                        idx += ((high - low) / 2);
                                        return await conq();
                                    }
                                    else if (dt > startDt)
                                    {
                                        //to far forward, need to back up and check again
                                        high = idx;
                                        idx -= ((high - low) / 2);
                                        return await conq();
                                    }
                                    else
                                    {
                                        //to be safe we need to search back to where this dt starts.
                                        for (int i = idx; i > 0; i--)
                                        {
                                            id = ids[idx];
                                            r = await _collection.TryGetValueAsync(tx, id);
                                            if (r.HasValue)
                                            {
                                                ev = r.Value;
                                                dt = ev.Timestamp;
                                                if (dt != startDt)
                                                {
                                                    return (i + 1);
                                                }
                                            }
                                        }
                                        //this means we went all the back to the start of the array
                                        return 0;
                                    }

                                }

                                return 0;
                            });
                            var startingIdx = await conq();

                            for (int i = startingIdx; i < ids.Count; ++i)
                            {
                                var id_x = ids[i];
                                if (maxCount != 0 && count > maxCount) break;
                                var r = await _collection.TryGetValueAsync(tx, id_x);
                                if (r.HasValue)
                                {
                                    var ev = r.Value;
                                    if (ev.Timestamp >= startDt && ev.Timestamp <= endDt)
                                    {
                                        count++;
                                        retVals.Add(r.Value);
                                    }
                                }
                            }
                        }
                       
                    }
                    
                }
            }

            return retVals;
        }
    }
}
