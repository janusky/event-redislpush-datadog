FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build

WORKDIR /app

# Crea el proyecto de consola
RUN dotnet new console -n app-producer

# Cambia al directorio del proyecto
WORKDIR /app/app-producer

# Copia el archivo Program.cs desde tu directorio local
COPY Program.cs .

# Compila el proyecto
RUN dotnet publish -c Release -o out

# Usa la imagen de .NET Runtime para ejecutar la aplicación
FROM mcr.microsoft.com/dotnet/runtime:6.0

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos publicados desde la etapa de construcción
COPY --from=build /app/app-producer/out .

# Establece el comando por defecto para ejecutar la aplicación
ENTRYPOINT ["dotnet", "app-producer.dll"]