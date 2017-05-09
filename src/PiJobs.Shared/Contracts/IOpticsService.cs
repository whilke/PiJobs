using Microsoft.ServiceFabric.Services.Remoting;
using PiJobs.Shared.Optics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Contracts
{
    public class OpticsResult
    {
        public List<OpticsEvent> Events { get; set; }
        public long ContinuationToken { get; set; }
        public double ExecutionTime { get; set; }
        public string Error { get; set; }
    }

    public interface IOptics : IService
    {
        Task Add(OpticsEvent evt);
        Task<OpticsResult> Query(string query, DateTime startTime, DateTime endTime, long continuationToken);
        Task<int> QueryCount(string query, DateTime startTime, DateTime endTime);
        Task Purge();
        Task AddBatch(List<OpticsEvent> evts);
    }
}
