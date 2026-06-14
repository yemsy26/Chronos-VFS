# Usa la imagen oficial estable de Rust basada en Debian Bookworm
# para garantizar compatibilidad con versiones recientes de glibc y el Kernel.
FROM rust:bookworm

# Establece el directorio de trabajo donde se montará el código fuente
WORKDIR /usr/src/nvm_prototype

# Instala dependencias del sistema recomendadas para compilar herramientas
# de red o asincronía de bajo nivel en Linux (aunque monoio funciona out-of-the-box,
# clang/cmake suelen ser útiles para dependencias nativas en el ecosistema zero-copy).
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    clang \
    && rm -rf /var/lib/apt/lists/*

# Mantenemos el contenedor vivo para poder entrar a compilar/ejecutar comandos iterativamente.
CMD ["tail", "-f", "/dev/null"]
