using Akka.Actor;
using TreciProjekat.Models;

namespace TreciProjekat.Actors;

/// <summary>
/// Aktor koji vrsi "kompleksniju obradu podataka" - agregatnu statistiku po autoru
/// (prosecan rejting, opseg godina izdavanja, distribucija jezika prevoda).
/// Namerno zivi na posebnom dispatcheru (vidi CoordinatorActor), tako da se
/// racunanje statistike izvrsava na drugoj niti nezavisno od AuthorActor-a i Rx tokova.
/// </summary>
public class StatsWorkerActor : ReceiveActor
{
    public StatsWorkerActor()
    {
        Receive<ComputeStats>(msg =>
        {
            try
            {
                var books = msg.Books;

                if (books.Count == 0)
                {
                    Sender.Tell(new StatsComputed(
                        msg.AuthorName,
                        new BookStats(0, null, null, null, new Dictionary<string, int>())));
                    return;
                }

                var ratings = books
                    .Where(b => b.RatingsAverage.HasValue)
                    .Select(b => b.RatingsAverage!.Value)
                    .ToList();

                double? overallAverage = ratings.Count > 0
                    ? Math.Round(ratings.Average(), 2)
                    : null;

                var years = books
                    .Where(b => b.FirstPublishYear.HasValue)
                    .Select(b => b.FirstPublishYear!.Value)
                    .ToList();

                int? earliest = years.Count > 0 ? years.Min() : null;
                int? latest = years.Count > 0 ? years.Max() : null;

                var languageDistribution = books
                    .SelectMany(b => b.Languages)
                    .GroupBy(l => l)
                    .ToDictionary(g => g.Key, g => g.Count());

                var stats = new BookStats(books.Count, overallAverage, earliest, latest, languageDistribution);

                Sender.Tell(new StatsComputed(msg.AuthorName, stats));
            }
            catch (Exception ex)
            {
                Sender.Tell(new StatsFailed(msg.AuthorName, ex));
            }
        });
    }
}