using Microsoft.Owin.Security.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Owin;
using Microsoft.Owin.Security;
using System.Security.Claims;
using PiJobs.API.Auth;

namespace PiJobs.API.Auth
{
    public class MyBasicAuth : OwinMiddleware
    {
        public MyBasicAuth(OwinMiddleware next) : base(next)
        {
        }

        public override async Task Invoke(IOwinContext context)
        {
            var hasAdmin = context.Request.Path.ToString().Contains("/admin");
            if (!hasAdmin)
            {
                await Next.Invoke(context);
            }
            else
            {
                var request = context.Request;
                var resp = context.Response;
                var header = request.Headers.Get("Authorization");

                if (!String.IsNullOrWhiteSpace(header))
                {
                    var authHeader = System.Net.Http.Headers
                                       .AuthenticationHeaderValue.Parse(header);

                    if ("Basic".Equals(authHeader.Scheme,
                                             StringComparison.OrdinalIgnoreCase))
                    {
                        string parameter = Encoding.UTF8.GetString(
                                              Convert.FromBase64String(
                                                    authHeader.Parameter));
                        var parts = parameter.Split(':');

                        string userName = parts[0];
                        string password = parts[1];

                        if (password == "secret") // Just a dumb check
                        {
                            var claims = new[]
                            {
                                new Claim(ClaimTypes.Name, "Admin")
                            };
                            var identity = new ClaimsIdentity(claims, "Basic");
                            request.User = new ClaimsPrincipal(identity);

                            await Next.Invoke(context);
                            return;
                        }
                    }
                }

                resp.StatusCode = 401;
                resp.Headers.Add("WWW-Authenticate", new string[] { "Basic" });
            }
        }
    }
}
