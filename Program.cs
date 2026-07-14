using TreciProjekat.Web;

namespace TreciProjekat;

public class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("=== OPENLIBRARY AUTHOR-BOOKS SERVER STARTUP ===");
        try
        {
            var server = new Server();
            server.StartAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"KRITICNA GRESKA PRI POKRETANJU: {ex.Message}");
        }

        Console.WriteLine("Aplikacija je ugasena.");
    }
}