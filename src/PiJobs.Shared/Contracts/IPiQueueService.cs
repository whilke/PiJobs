using Microsoft.ServiceFabric.Services.Remoting;
using PiJobs.Shared.Jobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Contracts
{
    public interface IPiQueueService : IService
    {
        Task AddTask(DataSession session, string digits);
        Task<JobState> GetStatus(DataSession session);
        Task UpdateStatus(DataSession session, JobState newState);
        Task RemoveStatus(DataSession session);
        Task<int> GetQueueSize();
    }
}
