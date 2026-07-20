using TreciProjekat.Models;
using System.Net.Http;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text.Json;

namespace TreciProjekat.Services;

public class RxService
{
    private readonly HttpClient _httpClient;
    private readonly AppSettings _settings;

    public RxService(HttpClient httpClient, AppSettings settings)
    {
        _httpClient = httpClient;
        _settings = settings;
    }

    public IObservable<IList<BookInfo>> StreamBooksPeriodically(string author)
    {
        return Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(_settings.RxIntervalSeconds))
            .Do(tick =>
            {
                var time = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                Console.WriteLine($"[Rx TAJMER] Vreme: {time} | Autor: '{author}' | Ciklus br: {tick + 1}");
            })
            .ObserveOn(TaskPoolScheduler.Default)
            .SelectMany(_ => FetchAllPagesParallel(author));
    }

    private IObservable<IList<BookInfo>> FetchAllPagesParallel(string author)
    {
        var pageSize = _settings.PageSize;

        return Observable.FromAsync(() => FetchPageAsync(author, offset: 0, limit: pageSize))
            .SelectMany(firstPage =>
            {
                var books = (firstPage.Docs ?? new List<OpenLibraryDoc>())
                    .Where(doc => !string.IsNullOrWhiteSpace(doc.Title))
                    .Select(doc => new BookInfo(
                        Key: doc.Key ?? Guid.NewGuid().ToString(),
                        Title: doc.Title!,
                        FirstPublishYear: doc.FirstPublishYear,
                        Languages: doc.Language ?? new List<string>(),
                        RatingsAverage: doc.RatingsAverage,
                        RatingsCount: doc.RatingsCount
                    ))
                    .ToList();

                var totalPagesAvailable = pageSize > 0
                    ? (int)Math.Ceiling((double)firstPage.NumFound / pageSize)
                    : 1;
                var pagesToFetch = Math.Min(Math.Max(totalPagesAvailable, 1), _settings.MaxPages);

                if (pagesToFetch <= 1)
                {
                    return Observable.Return((IList<BookInfo>)books);
                }

                var remainingOffsets = Enumerable.Range(1, pagesToFetch - 1)
                    .Select(page => page * pageSize);

                Console.WriteLine($"[Rx PARALELNO] Autor '{author}': povlacim jos {remainingOffsets.Count()} " +
                                   $"stranica ISTOVREMENO (offseti: {string.Join(", ", remainingOffsets)}).");

                return remainingOffsets
                    .ToObservable()
                    .SelectMany(offset => Observable.FromAsync(() => FetchPageAsync(author, offset, pageSize)))
                    .SelectMany(response => (response.Docs ?? new List<OpenLibraryDoc>())
                        .Where(doc => !string.IsNullOrWhiteSpace(doc.Title))
                        .Select(doc => new BookInfo(
                            Key: doc.Key ?? Guid.NewGuid().ToString(),
                            Title: doc.Title!,
                            FirstPublishYear: doc.FirstPublishYear,
                            Languages: doc.Language ?? new List<string>(),
                            RatingsAverage: doc.RatingsAverage,
                            RatingsCount: doc.RatingsCount
                        )))
                    .ToList()
                    .Select(restBooks =>
                    {
                        books.AddRange(restBooks);
                        return (IList<BookInfo>)books;
                    });
            })
            .Catch<IList<BookInfo>, Exception>(ex =>
            {
                Console.WriteLine($"[RxService UPOZORENJE] Greska pri dohvatanju za autora '{author}': {ex.Message}");
                return Observable.Return((IList<BookInfo>)new List<BookInfo>());
            });
    }

    private async Task<SearchResponse> FetchPageAsync(string author, int offset, int limit)
    {
        var fields = "key,title,first_publish_year,language,ratings_average,ratings_count,author_name";
        var url = $"https://openlibrary.org/search.json?author={Uri.EscapeDataString(author)}" +
                   $"&fields={fields}&limit={limit}&offset={offset}";

        try
        {
            var response = await _httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            var result = JsonSerializer.Deserialize<SearchResponse>(
                json,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            return result ?? throw new InvalidOperationException("API je vratio prazan odgovor nakon deserijalizacije.");
        }
        catch (HttpRequestException ex)
        {
            Console.WriteLine($"[Network Error] Problem sa konekcijom do {url}: {ex.Message}");
            throw;
        }
        catch (JsonException ex)
        {
            Console.WriteLine($"[JSON Error] Problem sa formatom podataka: {ex.Message}");
            throw new InvalidDataException("API odgovor nije u ocekivanom formatu.", ex);
        }
    }
}