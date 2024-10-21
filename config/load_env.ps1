$OutputEncoding = [System.Text.Encoding]::UTF8

# Importar PSReadLine para colorear la salida en la consola
if (-not (Get-Module -Name PSReadLine -ErrorAction SilentlyContinue)) {
    Import-Module PSReadLine
}

# Ruta al archivo .env
$envFilePath = ".env"

Get-Content $envFilePath | ForEach-Object {
    if ($_ -notmatch '^\s*#' -and $_ -match '^\s*(\w+)\s*=\s*(.*)\s*$') {
        $name = $matches[1]
        $value = $matches[2]
        [System.Environment]::SetEnvironmentVariable($name, $value)
    }
}

# Función para imprimir texto en color verde
function Write-Green {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Green
}

# Función para imprimir texto en color amarillo
function Write-Yellow {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Yellow
}

# Función para imprimir texto en color rojo
function Write-Red {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Red
}

# Obtener el nombre de la base de datos desde la variable de entorno
$databaseName = $env:DB_NAME

# Intentar ejecutar el bloque de código
try {
    if ($databaseExists -eq "1") {
        Write-Yellow "La base de datos '$databaseName' ya existe."
    } else {
        # Preparar argumentos para crear la base de datos
        $createDbArgs = @(
            "-U", $env:DB_USER,
            "-E", "UTF8",
            "--lc-collate=es_ES.UTF-8",
            "--lc-ctype=es_ES.UTF-8",
            "--template=template0",
            $databaseName
        )

        try {
            Write-Green "Creando base de datos..."
            $createDbProcess = Start-Process -FilePath "createdb" -ArgumentList $createDbArgs -NoNewWindow -Wait -PassThru -RedirectStandardOutput "../logs/createDbOutput.txt" -RedirectStandardError "../logs/createDbError.txt"

            if ($createDbError -match "ERROR:  la base de datos .*$databaseName.* ya existe") {
                Write-Yellow "La base de datos '$databaseName' ya existe."
            } elseif ($createDbProcess.ExitCode -eq 0) {
                Write-Green "Base de datos '$databaseName' creada con éxito."
            } else {
                Write-Yellow "La base de datos '$databaseName' ya existe"
            }
        } catch {
            Write-Red "Error al crear la base de datos: $_"
        }
    }
}
catch [System.Management.Automation.MethodInvocationException] {
    Write-Host ""
    Write-Red "Error al ejecutar el comando psql. Asegúrate de tener psql instalado y configurado correctamente."
}
catch {
    Write-Host ""
    Write-Red "Se produjo un error desconocido al inicializar la base de datos: $_"
}


finally {
    # Finalmente, independientemente del resultado, proceder con la inicialización de la base de datos si existe
    $initFilePath = "$(Split-Path -Parent $PSScriptRoot)/initdb/init.sql"
    
    # Mostrar la ruta donde se buscará el archivo init.sql
    Write-Yellow "`nBuscando init.sql en: $initFilePath`n"
    
    # Verificar si el archivo init.sql existe en la ruta especificada
    if (Test-Path $initFilePath) {
        # Si existe, ejecutar el script SQL para inicializar la base de datos
        Write-Green "Inicializando base de datos..."
        psql -U $env:DB_USER -d $databaseName -f $initFilePath
        # Definir emojis usando caracteres Unicode
        $SMILE = [char]::ConvertFromUtf32(0x1F600)     # 😀
        $CELEBRATION = [char]::ConvertFromUtf32(0x1F389)  # 🎉
        Write-Green "`n$SMILE La base de datos se ha inicializado correctamente $CELEBRATION `n`n`n"
    } else {
        # Si no existe, mostrar un mensaje de error
        Write-Red "No se encontró el archivo de inicialización en la ruta especificada: $initFilePath"
    }
}