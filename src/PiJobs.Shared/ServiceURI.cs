using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiJobs.Shared
{
    public static class ServiceURI
    {
        private const string _appUri = "fabric:/PiJobs/";
        private const string _queueUri = "PiQueue";
        private const string _dataUri = "PiDataSession";

        public static string OpticsService => _appUri + nameof(OpticsService);
        public static string Router => _appUri + nameof(Router);
        public static string PiQueue(DataSession session)
        {
            string uri = $"{_appUri}{_queueUri}_{session.Account}";
            return uri;
        }

        public static string PiDataSession(DataSession session)
        {
            string uri = $"{_appUri}{_dataUri}_{session.Account}_{session.DataId}";
            return uri;
        }

    }
}

