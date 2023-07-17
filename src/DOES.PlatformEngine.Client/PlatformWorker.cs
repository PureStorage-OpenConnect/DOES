using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Systemd;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace DOES.PlatformEngine.Client
{
    public class PlatformWorker : BackgroundService
    {
        private readonly ILogger<PlatformWorker> _logger;

        public PlatformWorker(ILogger<PlatformWorker> logger, IHostLifetime lifetime)
        {
            _logger = logger;
            _logger.LogInformation("IsSystemd: {isSystemd}", lifetime.GetType() == typeof(SystemdLifetime));
            _logger.LogInformation("IHostLifetime: {hostLifetime}", lifetime.GetType());
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogCritical("Critical log is running. Disregard this message");
            while (!stoppingToken.IsCancellationRequested)
            {
                //_logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}