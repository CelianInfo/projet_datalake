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



 ## Gestionnaire de librairie python : uv
 https://docs.astral.sh/uv/

 Sert à créer un environnement virtuel.
 Extrêmement similaire à Peotry, mais beaucoup plus performant.

Depuis le cmd (ouvert en tant qu'admin)
```
pip install uv
```

## ETL / Orchestrateur
https://docs.dagster.io/getting-started

Permet de créer des pipelines en Python avec énormément de flexibilité.
