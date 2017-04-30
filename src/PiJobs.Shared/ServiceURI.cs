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

        public static string Optics => _appUri + nameof(Optics);
    }
}

