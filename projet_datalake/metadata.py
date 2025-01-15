###############################################################################
#==============================================================================
#** TRT01 **
#------------------------------------------------------------------------------
#- Comment recuperer la date/time systeme
#==============================================================================
###############################################################################
from datetime import datetime
def Get_datetime():
    Result = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return(Result)

print(Get_datetime())  




###############################################################################
#==============================================================================
#** TRT02 **
#------------------------------------------------------------------------------
#- Comment parcourir un dossier et stocker dans une liste, les noms de fichiers
#   filtrés selon des critères choisis
#==============================================================================
###############################################################################
#-- Importation des bibliotheques necessaires
import os, fnmatch

#-- Initialisation des variables
myListOfFileSourceTmp = []
myListOfFileSource = []


#-- Utiliser dans le code comme sparateur d'arborescence des dossier soit "/" soit "\\"
#   mais jamais "\"
myPathSource = "C:/Users/eelmakoul/Documents/GitHub/projet_datalake/TD_DATALAKE/DATALAKE/0_SOURCE_WEB"


#-- Recupere dans une liste temporaire, les noms longs  des fichiers dans le path
myListOfFileSourceTmp = os.listdir(myPathSource)
liste_cible=["LINKEDIN/EMP","GLASSDOOR/SOC","GLASSDOOR/AVI"]
liste_source=["INFO-EMP","INFO-SOC","AVIS-SOC"]

ID_Object = 0
#-- Parametrage permettant de ne filtrer que les fichiers concernés parmis tous
#   les noms de fichiers de la liste
for i in range(3):
    myListOfFileSource = []
    myPattern = "*"+liste_source[i]+ "*.html"
   
    #-- Parcourt tous les fichiers trouvés contenus dans la liste temporaire
    #   et ajoute ceux correspondant au filtre dans la liste definitive
    for myFileNameTmp in myListOfFileSourceTmp :  
        #-- On n'ajoute que les fichiers concernés
        if fnmatch.fnmatch(myFileNameTmp, myPattern)==True:
            myListOfFileSource.append(myFileNameTmp)
   
    #-- Affichage à l'écran du nom des fichiers fichiers contenus
    #   dans la liste définitive
    for myFileName in myListOfFileSource : print(myFileName)
   
   
   
   
    ###############################################################################
    #==============================================================================
    #** TRT03 **
    #------------------------------------------------------------------------------
    #-- Comment preparer les informations et les lignes à enregistrer dans votre
    #   fichier de metadonnees techniques
    #==============================================================================
    ###############################################################################
    #-- Importation des bibliotheques necessaires
    import csv
   
   
    #-- Verification visuelle que la liste des fichiers à recopier est correcte
    for myFileName in myListOfFileSource : print(myFileName)
   
   
    #-- Initialisation des variables
   
    myListOfLigneToWrite = []
    #-- Creation de la 1ere ligne particulière du fichier (entete)
    myEnteteLst = ["id_objet","type_valeur","valeur"]
    myListOfLigneToWrite.append(myEnteteLst)
   
    print(myListOfLigneToWrite)
   
   
   
    #-- Exemple de boucle de traitment d'ecriture du fichier metadonnes techniques
    for myFileNameToCopy in myListOfFileSource:
        #-- Repertoire source
        myPathSource = "C:/Users/eelmakoul/Documents/GitHub/projet_datalake/TD_DATALAKE/DATALAKE/0_SOURCE_WEB"
       
        #!!! Attention, dans cet exemple,  on ne traite que les données EMP  !!!
        #==> A adapter et compléter le code pour gérer les 2 autres types de données SOC, AVI
        #-- Repertoire cible
        myPathCible = "C:/Users/eelmakoul/Documents/GitHub/projet_datalake/TD_DATALAKE/DATALAKE/1_LANDING_ZONE" + liste_cible[i]
   
        myPathFileNameSource = myPathSource + "/" + myFileNameToCopy
        myPathFileNameCible = myPathCible + "/" + myFileNameToCopy
       
        #-- Creation d'une ligne pour la date d'ingestion
        myLigneLst = [str(ID_Object), "date_ingestion", Get_datetime()]
        myListOfLigneToWrite.append(myLigneLst)
       
        #-- Creation d'une ligne pour l'emplacement d'où l'on a récupéré le fichier
        myLigneLst = [str(ID_Object), "path_name_file_source", myPathFileNameSource]
        myListOfLigneToWrite.append(myLigneLst)
       
        #-- Creation d'une ligne pour l'emplacement où l'on a enregistré le fichier
        myLigneLst = [str(ID_Object), "path_name_file_cible", myPathFileNameCible]
        myListOfLigneToWrite.append(myLigneLst)
   
       
        #-- Rajoutez d'autres métadonnées techniques si necessaire
        # ....    
        # ....
   
        ID_Object = ID_Object + 1
   
    #-- Verifiation visuelle contenue de la liste des metadonnées à eregistrer
    for myLigne in myListOfLigneToWrite : print(myLigne, "\n")
   
   
   
   
    ###############################################################################
    #==============================================================================
    #** TRT04 **
    #------------------------------------------------------------------------------
    #-- Ecriture directe de la liste le fichier CSV de metadonnees techniques
    #    RQ: Ici la liste contiendra les lignes pour SOC, AVI et EMP qui seront
    #         enregistrées à ce moment dans leur totalité
    #==============================================================================
    ###############################################################################
   
    #------------------------------------------------------------------------------
    #------------------------------------------------------------------------------
    #-- Definir le nom et l'emplacement où sera stocké votre fichier metadonnes techniques
    myPathMetaData = "C:/Users/eelmakoul/Documents/GitHub/projet_datalake/TD_DATALAKE/DATALAKE/99_METADATA"
    myFileNameMetaDataTech = "metadata_technique.csv"
    myPathFileNameMetaDataTech = myPathMetaData + "/" + myFileNameMetaDataTech
   
    #-- Verification visuelle  u nom de fichier métadonnées techniques
    print(myPathFileNameMetaDataTech)
   
    #-- Affichage pour controle des ligne contenues dans la liste  enregistrer dans le fichier
    for myLigne in myListOfLigneToWrite :  print(myLigne)
   
    #-- Ouverture du fihier en append ("a" = ajout)
    # myPtrFile = open(myPathFileNameMetaDataOut, 'a', newline='', encoding="utf-8", errors="ignore")
    myPtrFile = open(myPathFileNameMetaDataTech, 'a', newline='')
   
    #-- Ecriture en une seule commade de l'ensemble ds lignes contenues dans la liste que vous avez préparé
    myWriter = csv.writer(myPtrFile, delimiter=';', quotechar='"',  quoting=csv.QUOTE_ALL, lineterminator='\n')
   
    myWriter.writerows(myListOfLigneToWrite)
   
    #-- Fermeture du fihier
    myPtrFile.close()
   
   
   
   
    ###############################################################################
    #==============================================================================
    #** TRT05 **
    #------------------------------------------------------------------------------
    #-- Comment Copier TOUS les fichiers de la liste d'un repertoire dans un autre repertoire
    #==============================================================================
    ###############################################################################
    #-- Importation des bibliotheques necessaires
    import shutil
   
    #-- Verification visuelle que la liste des fichiers à recopier est correcte
    for myFileName in myListOfFileSource :
        print(myFileName)
   
    #-- Initialisation des variables
    #-- Preciser le repertoire source
    myPathSource = "C:/Users/eelmakoul/Documents/GitHub/projet_datalake/TD_DATALAKE/DATALAKE/0_SOURCE_WEB"
   
    #-- Preciser le repertoire cible : (Exemple ici, pour recopier les fichiers EMP a partir de la liste correspondante)
    myPathCible = "C:/Users/eelmakoul/Documents/GitHub/projet_datalake/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/" + liste_cible[i]
   
    #------------------------------------------------------------------------------
    #-- !!! Ecrire ici l'entete dans le fichier de Metadonnes Techniques
    #       ==> Ne le faire qu'une seule fois à la 1ere creation du fichier)
    #------------------------------------------------------------------------------
   
    print("******** Debut de copie des fichiers ********")
   
    #-- Boucle pour copier l'ensemble des fichiers contenus dans les listes des fichiers à recopier
    #   ==> A compléter et adapter pour gérer pour les 3 listes SOC, AVI et EMP
   
    for myFileNameToCopy in myListOfFileSource:
        #-- Preparation et affectation dans des variables
        #    des chemins complets des fchiers source et cible
        myPathFileNameSource = myPathSource + "/" + myFileNameToCopy
        myPathFileNameCible = myPathCible + "/" + myFileNameToCopy
       
        print("Copie du fichier : ", "\t", myPathFileNameSource, " -- vers -->", myPathFileNameCible, "\n")
        #-- Lancement de la commande de copie du fichier
        shutil.copy(myPathFileNameSource, myPathFileNameCible)
   
        #---------------------------------------------------
        # Vous coderez ici une  partie enregistrement des METADONNEES TECHNIQUE :
        #
        #   - Recuperation de la date / Heure systeme
        #   - Preparation des lignes à enregistrer dans le fichier Metadonnees Techniques
        #   - Ouverture en append (ajout) du fichier et enregistrement des lignes formatees comme vu en cours
        #     et fourni dans vos ressources à dispositin sur le site u mooc
        #   - ...
        #   - fermeture du fichier Metadonnees Techniques
        #---------------------------------------------------    
       
       
        #---------------------------------------------------
       
    print("******** Fin de copie des fichiers ********\n")