using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared.Optics
{
    public class OpticsClient
    {

        public async Task<List<OpticsEvent>> Query(DateTime start, DateTime end, params KeyValuePair<string,string>[] query)
        {
            long ctoken = 0;
            List<OpticsEvent> events = new List<OpticsEvent>();
            do
            {
                var results = await ServiceResolver.Optics.Query(start, end, query.ToList(), ctoken);
                events.AddRange(results.Items);
                ctoken = results.ContinueToken;
            } while (ctoken != 0);

            return events;
        }
    }
}
