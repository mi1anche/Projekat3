using Akka.Actor;
using Akka.Event;
using TreciProjekat.Actors;
using TreciProjekat.Models;
using TreciProjekat.Services;

namespace TreciProjekat.Actors;

public class CoordinatorActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();

    private readonly Dictionary<string, IActorRef> _authorActors = new();
    private readonly AppSettings _settings;

    private static readonly HttpClient SharedHttpClient = new HttpClient();

    public CoordinatorActor(AppSettings settings)
    {
        _settings = settings;
        Receive<FetchBooksByAuthor>(request =>
        {
            var authorKey = request.AuthorName.Trim().ToLowerInvariant();

            if (!_authorActors.TryGetValue(authorKey, out var authorActor))
            {
                _log.Info($"[Coordinator] Kreiram infrastrukturu za novog autora '{request.AuthorName}'.");

                var safeName = SanitizeActorName(authorKey);

                var workerProps = Props.Create(() => new StatsWorkerActor())
                    .WithDispatcher("akka.stats-dispatcher");
                var statsWorker = Context.ActorOf(workerProps, $"stats-worker-{safeName}");

                var rxService = new RxService(SharedHttpClient, _settings);
                var authorProps = Props.Create(() => new AuthorActor(request.AuthorName, statsWorker, rxService));

                authorActor = Context.ActorOf(authorProps, $"author-{safeName}");
                _authorActors[authorKey] = authorActor;
            }

            authorActor.Forward(request);
        });
    }

    protected override SupervisorStrategy SupervisorStrategy()
    {
        return new OneForOneStrategy(
            maxNrOfRetries: 10,
            withinTimeRange: TimeSpan.FromMinutes(1),
            decider: Decider.From(ex =>
            {
                if (ex is ArgumentException || ex is InvalidOperationException)
                    return Directive.Resume;

                if (ex is OutOfMemoryException)
                    return Directive.Stop;

                return Directive.Restart;
            })
        );
    }

    private static string SanitizeActorName(string raw)
    {
        var chars = raw.Select(c =>
            char.IsLetterOrDigit(c) && c <= 127 ? c : '-'
        ).ToArray();

        var result = new string(chars);

        while (result.Contains("--"))
            result = result.Replace("--", "-");

        return result.Trim('-');
    }
}