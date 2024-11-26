using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

class Program {
    static async Task Main(string[] args) {
        // Ruta al directorio donde se encuentran los archivos JSON
        string jsonDirectory = @"./json_files"; // Asegúrate de tener este directorio

        // Verificar que el directorio existe
        if (!Directory.Exists(jsonDirectory)) {
            Console.WriteLine($"El directorio '{jsonDirectory}' no existe.");
            return;
        }

        // Obtener todos los archivos JSON en el directorio
        string[] jsonFiles = Directory.GetFiles(jsonDirectory, "*.json");

        using var client = new HttpClient();

        foreach (var filePath in jsonFiles) {
            try {
                // Leer el contenido del archivo JSON
                string jsonContent = await File.ReadAllTextAsync(filePath);
                Console.WriteLine($"Enviando evento del archivo: {filePath}");

                var PRODUCER_HOST = Environment.GetEnvironmentVariable("PRODUCER_HOST");
                var response = await client.PostAsync($"http://{PRODUCER_HOST}:5000/event", new StringContent(jsonContent, Encoding.UTF8, "application/json"));

                if (response.IsSuccessStatusCode) {
                    Console.WriteLine("Evento enviado exitosamente!");
                } else  {
                    Console.WriteLine($"Error al enviar el evento desde '{filePath}': {response.StatusCode}");
                }
            } catch (Exception ex) {
                Console.WriteLine($"Error al procesar el archivo '{filePath}': {ex.Message}");
            }
        }
    }
}