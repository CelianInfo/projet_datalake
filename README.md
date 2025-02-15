# projet_datalake
Projet du cours de gestion de Données Massives avec Eric Kloeckle

## Fichiers mis à dispositions

Archive **TD_DATALAKE**

```bash
├───DATALAKE
│   ├───0_SOURCE_WEB # Fichiers HTML de départ
│   ├───1_LANDING_ZONE
│   │   ├───GLASSDOOR
│   │   │   ├───AVI
│   │   │   └───SOC
│   │   └───LINKEDIN
│   │       └───EMP
│   ├───2_CURATED_ZONE
│   │   ├───GLASSDOOR
│   │   │   ├───AVI
│   │   │   └───SOC
│   │   └───LINKEDIN
│   │       ├───EMP
│   │       └───SOC
│   └───3_PRODUCTION_ZONE
│       └───BDD
├───DATAVIZ
├───DVLP
├───ETL
└───LOGFILES
```

## Installation prolet

### Python

Installer Python 3.12

 ### Gestionnaire de librairie python : uv
 https://docs.astral.sh/uv/

 Sert à créer un environnement virtuel.
 Extrêmement similaire à Peotry, mais écrit en rust et beaucoup plus performant.

Depuis le cmd (ouvert en tant qu'admin)
```
pip install uv
```
### Créer une base duckdb

Installer le CLI DuckDB.

Sous un CMD ou Powershell

```
winget install DuckDB.cli
```

Une fois installé fermer le shell et ouvrir un nouveau shell dans TD_DATALAKE\DATALAKE\3_PRODUCTION_ZONE\, puis taper

```
duckdb /database.duckdb
```

Ferme le shell

### Lancer le projet

Ouvrir un shell dans le répertoire du projet, puis lancer :

```
uv run dagster dev
```

Automatiquement tout les modules python vont s'installer dans un environement virtuel (dossier .venv).

Une fois fini une url vers le serveur Dagster va s'ouvrir. Par défaut http://127.0.0.1:3000
Crlt+click pour l'ouvrir dans un navigateur (ou l'écrire soi-même).

### Lancer les transformations

Cliquer sur le bouton **Materialize All** en haut à droite.

### Voir le résultat

Ouvrir la base duckdb (TD_DATALAKE\DATALAKE\3_PRODUCTION_ZONE\database.duckdb) à l'aide de n'importe quel explorateur de base de données.

DBeaver permet de l'ouvrir directement.
Il suffit de choisir DuckDB et de naviger vers le fichier.
