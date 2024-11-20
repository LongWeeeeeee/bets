import yt_dlp
import os

# Список песен
songs = songs = [
    "Daniel Santacruz - Hello.mp3",
    "Daniel Santacruz - Lo Dice La Gente.mp3",
    "Daniel Santacruz, Mario Baro - Tropical.mp3",
    "Daniel Santacruz, sP Polanco - Baile Asesino.mp3",
    "Denovah, Yised - Salvacion.mp3",
    "Diego Ferrani - Salvame.mp3",
    "DJ Tony Pecino - Tu Me Delatas.mp3",
    "DJ Tony Pecino, Román - La Reina (Bachata Version).mp3",
    "DJ Tronky, Manny Rod - El Merengue (Bachata Version).mp3",
    "Esme - Bestia Salvaje.mp3",
    "ESME - Botella.mp3",
    "Esme - Materialista.mp3",
    "Esme - Soy Aquel.mp3",
    "Esme, Dioris, Felix, Chantel - Loco Por Ti.mp3",
    "Esme, J-Style - El Mismo Infeliz.mp3",
    "Felix - Solo Mia.mp3",
    "Felix - Todavia.mp3",
    "Grupo Extra - Lloras.mp3",
    "Grupo Extra - Quiereme un Poquito.mp3",
    "Grupo Extra - Tengo una Necesidad.mp3",
    "Grupo Extra - Traicionera.mp3",
    "Grupo Extra - Una Lady Como Tu.mp3",
    "Grupo Extra, Daniel Santacruz - Volvieron A Darme Las 6.mp3",
    "Hector Acosta - Amorcito Enfermito.mp3",
    "Hector Acosta - Antes Del Lunes.mp3",
    "Hector Acosta - Me Duele la Cabeza.mp3",
    "Hector Acosta - Pa' Que Me Perdones.mp3",
    "Hector Acosta, Jory Boy - Mala Suerte - Ya Que Te Vas.mp3",
    "Henry Santos, Luis Vargas - Una Mentirita.mp3",
    "Jay Ramirez, Daniel Santacruz - Si Volviera A Nacer.mp3",
    "Jeyro, Maykel - Solo Tu.mp3",
    "Jhonny Evidence, DJ Khalid - Palomita Voladora.mp3",
    "Jhonny Evidence, Marco Puma - Desnuda.mp3"
]


# Папка для загрузки
output_dir = "downloads2"
os.makedirs(output_dir, exist_ok=True)

def download_song(song_name):
    search_query = f"ytsearch:{song_name}"
    ydl_opts = {
        'format': 'bestaudio/best',
        'noplaylist': True,
        'extractaudio': True,
        'postprocessors': [
            {
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }
        ],
        'outtmpl': os.path.join(output_dir, '%(title)s.%(ext)s'),
        'quiet': False,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([search_query])
    except Exception as e:
        print(f"Error downloading {song_name}: {e}")

# Скачиваем все песни из списка
for song in songs:
    download_song(song)
