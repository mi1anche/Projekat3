using System.Text.Json.Serialization;

namespace TreciProjekat.Models;

public record OpenLibraryDoc(
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("title")] string? Title,
    [property: JsonPropertyName("first_publish_year")] int? FirstPublishYear,
    [property: JsonPropertyName("language")] List<string>? Language,
    [property: JsonPropertyName("ratings_average")] double? RatingsAverage,
    [property: JsonPropertyName("ratings_count")] int? RatingsCount,
    [property: JsonPropertyName("author_name")] List<string>? AuthorName
);

public record SearchResponse(
    [property: JsonPropertyName("numFound")] int NumFound,
    [property: JsonPropertyName("start")] int Start,
    [property: JsonPropertyName("docs")] List<OpenLibraryDoc>? Docs
);


// ako API promeni json strukturu da ne moramo da menjamo ceo sistem, vec samo mapiranje
public record BookInfo(
    string Key,
    string Title,
    int? FirstPublishYear,
    List<string> Languages,
    double? RatingsAverage,
    int? RatingsCount
);

public record BookStats(
    int TotalBooks,
    double? OverallAverageRating,
    int? EarliestYear,
    int? LatestYear,
    Dictionary<string, int> LanguageDistribution
);



public record BooksUpdated(IList<BookInfo> Books);
public record FetchBooksByAuthor(string AuthorName);
public record AuthorBooksResult(string Author, List<BookInfo> Books, BookStats? Stats);
public record ComputeStats(string AuthorName, List<BookInfo> Books);
public record StatsComputed(string AuthorName, BookStats Stats);
public record StatsFailed(string AuthorName, Exception Exception);

public record AppSettings(
    int PageSize = 20,
    int MaxPages = 3,
    int RxIntervalSeconds = 120
);