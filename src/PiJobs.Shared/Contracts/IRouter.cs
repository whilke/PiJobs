using Microsoft.ServiceFabric.Services.Remoting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Contracts
{
    public interface IRouter : IService
    {
        Task AddOrGet(DataSession session);
        Task Close(DataSession session);

        Task<List<string>> GetAccountList();
    }
}
