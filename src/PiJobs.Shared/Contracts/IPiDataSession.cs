using Microsoft.ServiceFabric.Services.Remoting;
using PiJobs.Shared.Jobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Contracts
{
    public interface IPiDataSession : IService
    {
        Task Init(DataSession session);
        Task StartJob(JobRecord job);
        Task<string> FetchData();
        Task Close();
    }
}
