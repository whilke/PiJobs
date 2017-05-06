using PiJobs.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

namespace PiJobs.API.Controllers
{
    [RoutePrefix("api/session")]
    public class SessionController: ApiController
    {
        [HttpGet]
        [Route("{account}/{user}/{data}")]
        public async Task<DataSession> Get(string account, string user, string data)
        {
            var ds = new DataSession(account, data, user);
            await ServiceResolver.Router.AddOrGet(ds);
            return ds;
        }

        [HttpDelete]
        [Route("{account}/{user}/{data}")]
        public Task Delete(string account, string user, string data)
        {
            var ds = new DataSession(account, data, user);
            return ServiceResolver.Router.Close(ds);
        }
    }
}
