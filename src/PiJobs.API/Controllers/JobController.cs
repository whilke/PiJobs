using PiJobs.Shared;
using PiJobs.Shared.Jobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace PiJobs.API.Controllers
{
    [RoutePrefix("api/job")]
    public class JobController : ApiController
    {

        [HttpPost]
        [Route("{account}/{user}/{data}")]
        public async Task Post(string account, string user, string data)
        {
            var ds = new DataSession(account, data, user);
            await ds.PiQueue().AddTask(ds);
        }

        [HttpGet]
        [Route("{account}/{user}/{data}")]
        public async Task<JobState> Get(string account, string user, string data)
        {
            var ds = new DataSession(account, data, user);
            return await ds.PiQueue().GetStatus(ds);
        }

        [HttpDelete]
        [Route("{account}/{user}/{data}")]
        public async Task Delete(string account, string user, string data)
        {
            var ds = new DataSession(account, data, user);
            await ds.PiQueue().RemoveStatus(ds);
        }
    }
}
