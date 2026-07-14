using Akka.Actor;
using Akka.Event;
using TreciProjekat.Models;
using TreciProjekat.Services;

namespace TreciProjekat.Actors;

public class AuthorActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private IDisposable _subscription = null!;

    private readonly string _authorName;
    private readonly RxService _rxService;
    private readonly IActorRef _statsWorker;

    private readonly Dictionary<string, BookInfo> _books = new();
    private BookStats? _lastStats;

    private readonly List<IActorRef> _pendingRequesters = new();

    public AuthorActor(string authorName, IActorRef statsWorker, RxService rxService)
    {
        _authorName = authorName;
        _rxService = rxService;
        _statsWorker = statsWorker;

        Receive<FetchBooksByAuthor>(msg =>
        {
            _log.Info($"Primljen zahtev za autora '{_authorName}'.");

            if (_books.Count > 0 && _lastStats != null)
            {
                Sender.Tell(new AuthorBooksResult(_authorName, _books.Values.ToList(), _lastStats));
            }
            else
            {
                _log.Info($"Podaci za '{_authorName}' se jos uvek prikupljaju. Zahtev stavljen na cekanje.");
                _pendingRequesters.Add(Sender);
            }
        });

        Receive<BooksUpdated>(msg =>
        {
            _log.Info($"[Rx] Azuriranje za '{_authorName}': primljeno {msg.Books.Count} knjiga.");

            foreach (var book in msg.Books)
            {
                _books[book.Key] = book;
            }
            _statsWorker.Tell(new ComputeStats(_authorName, _books.Values.ToList()));
        });

        Receive<StatsComputed>(msg =>
        {
            _log.Info($"[Stats] Statistika azurirana za '{_authorName}' " +
                      $"({msg.Stats.TotalBooks} knjiga, prosecan rejting: {msg.Stats.OverallAverageRating?.ToString() ?? "N/A"}).");

            _lastStats = msg.Stats;
            BroadcastToPending();
        });

        Receive<StatsFailed>(msg =>
        {
            _log.Error(msg.Exception, $"Greska pri racunanju statistike za '{_authorName}'.");
        });
    }

    private void BroadcastToPending()
    {
        if (_pendingRequesters.Count == 0) return;

        var result = new AuthorBooksResult(_authorName, _books.Values.ToList(), _lastStats);
        foreach (var requester in _pendingRequesters)
        {
            requester.Tell(result);
        }
        _pendingRequesters.Clear();
    }

    protected override void PreStart()
    {
        _log.Info($"AuthorActor pokrenut za '{_authorName}'. Pokrecem Rx stream.");

        var self = Self;
        _subscription = _rxService.StreamBooksPeriodically(_authorName)
            .Subscribe(
                books => self.Tell(new BooksUpdated(books)),
                ex => _log.Error(ex, "Kriticna Rx greska u AuthorActor-u.")
            );
    }

    protected override void PostStop()
    {
        _subscription?.Dispose();
        base.PostStop();
    }
}