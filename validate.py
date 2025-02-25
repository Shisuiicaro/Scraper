import aiohttp
import asyncio
import json
from colorama import Fore, init

init(autoreset=True)

REMOTE_JSON_URL = "https://raw.githubusercontent.com/Shisuiicaro/source/main/shisuyssource.json"
CONCURRENT_REQUESTS = 100
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive"
}

async def fetch_json(session):
    """Obtém o JSON remoto e o converte para objeto Python."""
    async with session.get(REMOTE_JSON_URL, headers=HEADERS) as response:
        if response.status != 200:
            print(f"{Fore.RED}Erro ao buscar JSON: Status {response.status}")
            return None
        text = await response.text()
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            print(f"{Fore.RED}Erro na conversão do JSON: {e}")
            return None

async def validate_single_link(session, link, semaphore):

    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(link, headers=HEADERS, timeout=timeout) as response:
                if response.status != 200:
                    print(f"{Fore.RED}[INVALID] {link} (Status {response.status})")
                    return None
                content = await response.text()
                error_indicators = [
                    "file could not be found",
                    "unavailable for legal reasons",
                    "unavailable",
                    "torrent",
                    "magnet:",
                    ".torrent",
                    "file has been deleted",  # Gofile
                    "file does not exist",    # Mediafire
                    "error-page-premium",     # Mediafire premium error
                    "file has been removed"   # Generic removal message
                ]

                # Verificação específica para Gofile
                if "gofile.io" in link.lower():
                    if "contentId" not in content or "Sorry, the file you are requesting does not exist" in content:
                        print(f"{Fore.RED}[INVALID] {link} (Arquivo Gofile não existe)")
                        return None

                # Verificação específica para Mediafire
                if "mediafire.com" in link.lower():
                    if "error.php" in response.url.path or "File Removed for Violation" in content:
                        print(f"{Fore.RED}[INVALID] {link} (Arquivo Mediafire removido)")
                        return None

                if any(indicator in content.lower() for indicator in error_indicators):
                    print(f"{Fore.RED}[INVALID] {link} (Indicador de erro detectado)")
                    return None
                    
                print(f"{Fore.GREEN}[VALID] {link}")
                return link
    except Exception as e:
        print(f"{Fore.RED}[ERROR] Falha ao validar {link}: {e}")
        return None

async def validate_all_links():

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        data = await fetch_json(session)
        if not data or "downloads" not in data or not isinstance(data["downloads"], list):
            print(f"{Fore.RED}JSON com estrutura inválida ou sem a chave 'downloads'.")
            return

        for game in data["downloads"]:
            uris = game.get("uris", [])
            results = await asyncio.gather(*(validate_single_link(session, uri, semaphore) for uri in uris))
            valid_links = [uri for uri in results if uri is not None]
            game["uris"] = valid_links


        def is_valid_game(game):
            links = game.get("uris", [])
            if not links:
                return False
            if all("1fichier.com" in link for link in links):
                return False
            if all("torrent" in link.lower() for link in links):
                return False
            return True

        data["downloads"] = [game for game in data["downloads"] if is_valid_game(game)]

        output_filename = "shisuyssource.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"{Fore.GREEN}Arquivo '{output_filename}' atualizado com sucesso.")

def main():
    asyncio.run(validate_all_links())

if __name__ == "__main__":
    main()
