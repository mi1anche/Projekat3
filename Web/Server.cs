using Akka.Actor;
using System.Net;
using System.Text;
using System.Text.Json;
using TreciProjekat.Actors;
using TreciProjekat.Models;

namespace TreciProjekat.Web;

public class Server
{
    private ActorSystem? _actorSystem;
    private IActorRef? _coordinator;
    private HttpListener _listener = new();
    private bool _isRunning = true;

    public async Task StartAsync()
    {
        var hocon = @"
        akka {
            loglevel = INFO
            stdout-loglevel = INFO
            loggers = [""Akka.Event.DefaultLogger""]

            log-dead-letters = 0
            log-dead-letters-during-shutdown = off

            stats-dispatcher {
                type = Dispatcher
                executor = ""fork-join-executor""
                fork-join-executor {
                    parallelism-min = 2
                    parallelism-max = 6
                }
                throughput = 1
            }
        }";

        _actorSystem = ActorSystem.Create("OpenLibrarySystem", hocon);
        var settings = LoadSettings();
        _coordinator = _actorSystem.ActorOf(Props.Create(() => new CoordinatorActor(settings)), "coordinator");

        _listener = new HttpListener();
        _listener.Prefixes.Add("http://localhost:5000/");
        _listener.Start();

        Log("INFO", "STARTUP", $"Server slusa na adresi: http://localhost:5000/");
        Console.WriteLine("Pritisni 'Q' za gasenje servera.");

        _ = Task.Run(ListenForShutdown);

        while (_isRunning)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                _ = Task.Run(() => HandleRequest(context));
            }
            catch (HttpListenerException) when (!_isRunning)
            {
            }
        }
    }

    private static AppSettings LoadSettings()
    {
        try
        {
            var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
            if (!File.Exists(path)) return new AppSettings();

            var json = File.ReadAllText(path);
            return System.Text.Json.JsonSerializer.Deserialize<AppSettings>(
                json, new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new AppSettings();
        }
        catch
        {
            return new AppSettings();
        }
    }

    private void ListenForShutdown()
    {
        while (_isRunning)
        {
            if (Console.ReadKey(true).Key == ConsoleKey.Q)
            {
                Console.WriteLine("\nGasenje servera...");
                StopAsync().GetAwaiter().GetResult();
                break;
            }
        }
    }

    private async Task HandleRequest(HttpListenerContext context)
    {
        var request = context.Request;
        var path = request.Url?.AbsolutePath.Trim('/');
        var clientIp = request.RemoteEndPoint?.ToString() ?? "unknown";

        Log("INFO", "REQUEST", $"Primljen GET zahtev sa putanjom '/{path}' od {clientIp}");

        if (path == "favicon.ico")
        {
            context.Response.StatusCode = 204;
            context.Response.Close();
            return;
        }

        if (string.IsNullOrWhiteSpace(path))
        {
            Log("WARN", "REQUEST", "Zahtev odbijen - nedostaje ime autora u putanji.");
            RespondWithText(context, "Nedostaje ime autora. Primer: http://localhost:5000/tolkien", 400);
            return;
        }

        var authorName = Uri.UnescapeDataString(path);

        try
        {
            var requestMsg = new FetchBooksByAuthor(authorName);

            var result = await _coordinator!.Ask<AuthorBooksResult>(requestMsg, TimeSpan.FromSeconds(60));

            Log("SUCCESS", "REQUEST", $"Zahtev za autora '{authorName}' uspesno obradjen. " +
                                       $"Broj knjiga: {result.Books.Count}, " +
                                       $"prosecan rejting: {result.Stats?.OverallAverageRating?.ToString() ?? "N/A"}.");

            RespondWithJson(context, result);
        }
        catch (TaskCanceledException)
        {
            Log("ERROR", "REQUEST", $"Zahtev za autora '{authorName}' - isteklo vreme cekanja (timeout).");
            RespondWithText(context, "Greska: isteklo vreme cekanja odgovora od aktora.", 504);
        }
        catch (Exception ex)
        {
            Log("ERROR", "REQUEST", $"Zahtev za autora '{authorName}' neuspesan: {ex.Message}");
            RespondWithText(context, "Greska na serveru: " + ex.Message, 500);
        }
    }

    private static void Log(string level, string category, string message)
    {
        var time = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        Console.WriteLine($"[{time}] [{level}] [{category}] {message}");
    }

    public async Task StopAsync()
    {
        Log("INFO", "SHUTDOWN", "Pokrecem graceful shutdown...");

        _isRunning = false;
        _listener?.Stop();

        if (_actorSystem != null)
        {
            await _actorSystem.Terminate();
        }

        Log("INFO", "SHUTDOWN", "Akka.NET sistem i HTTP server su uspesno ugaseni.");
    }

    private void RespondWithJson(HttpListenerContext context, object content, int statusCode = 200)
    {
        try
        {
            var response = context.Response;
            byte[] buffer = JsonSerializer.SerializeToUtf8Bytes(content, new JsonSerializerOptions { WriteIndented = true });

            response.StatusCode = statusCode;
            response.ContentType = "application/json";
            response.ContentLength64 = buffer.Length;
            response.OutputStream.Write(buffer, 0, buffer.Length);
        }
        finally
        {
            context.Response.Close();
        }
    }

    private void RespondWithText(HttpListenerContext context, string text, int statusCode = 200)
    {
        try
        {
            var response = context.Response;
            byte[] buffer = Encoding.UTF8.GetBytes(text);

            response.StatusCode = statusCode;
            response.ContentType = "text/plain";
            response.ContentLength64 = buffer.Length;
            response.OutputStream.Write(buffer, 0, buffer.Length);
        }
        finally
        {
            context.Response.Close();
        }
    }
}