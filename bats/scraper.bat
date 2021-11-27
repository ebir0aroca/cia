@echo off
echo starting scripts...
TITLE Webscrapper

ECHO ============================
ECHO 1. HORNBACH
ECHO Check all countries at https://www.hornbach.com/de/index.html
ECHO ============================
ECHO 1.1. hornbach.de
python.exe "../arv/sources/hornbach.py" "de" "https://www.hornbach.de/cms/de/de/sortiment/bad-sanitaer.html"
ECHO ============================
ECHO 1.2. hornbach.se
python.exe "../arv/sources/hornbach.py" "se" "https://www.hornbach.se/shop/Badrumsinredning/S18290/artikel-listning.html"
ECHO ============================
ECHO 1.3. hornbach.cz
python.exe "../arv/sources/hornbach.py" "cz" "https://www.hornbach.cz/shop/Koupelnovy-nabytek-a-zrcadla/S11572/seznam-zbozi.html"
ECHO ============================
ECHO 1.4. hornbach.sk
python.exe "../arv/sources/hornbach.py" "sk" "https://www.hornbach.sk/shop/Kupelnovy-nabytok-a-zrkadla/S16532/zoznam-tovaru.html"
ECHO ============================
ECHO 1.5. hornbach.ro
python.exe "../arv/sources/hornbach.py" "ro" "https://www.hornbach.ro/shop/Mobilier-baie/S19080/lista-produse.html"
ECHO ============================
ECHO 1.6. hornbach.at
python.exe "../arv/sources/hornbach.py" "at" "https://www.hornbach.at/shop/Badmoebel/S2307/artikelliste.html"
ECHO ============================
ECHO 1.7. hornbach.ch
python.exe "../arv/sources/hornbach.py" "ch" "https://www.hornbach.ch/shop/Badezimmermoebel/S6876/artikelliste.html"
ECHO ============================
ECHO 1.8. hornbach.lu (does not work)
ECHO python.exe "../arv/sources/hornbach.py" "lu" "https://www.hornbach.lu/fr/c/salles-de-bains-sanitaires/meubles-de-salle-de-bains/S10025/"
ECHO ============================
ECHO 1.9. hornbach.nl
python.exe "../arv/sources/hornbach.py" "nl" "https://www.hornbach.nl/shop/Badkamermeubelen-spiegels/S4387/artikeloverzicht.html"



ECHO All scripts have been processed.
pause
