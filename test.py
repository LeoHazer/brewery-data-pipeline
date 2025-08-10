import requests

r = requests.get("https://api.openbrewerydb.org/v1/breweries")

if r.status_code == 200 and r.headers.get("Content-Type") == "application/json":
    print(r.json())
else:
    print(f"Erro {r.status_code}: resposta não é JSON.")
