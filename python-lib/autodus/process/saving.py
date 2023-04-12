import re
from datetime import datetime
from autodus.exception.exceptions import SaveException

# Retourne la liste des duplications pour un projet donné
def getProjectDuplications(projectId,projectList):
    saves = []
    #récupération de toutes les sauvegardes
    for project in projectList:
        if re.match(r'^SAVE[0-9]{6,}__'+projectId,project) :
            saves.append(project)
    return saves

# Retourne le dernier projet dupliqué d'un projet (s'il existe)
def getLastDuplication(projectId,client):
    saves = getProjectDuplications(projectId,client.list_project_keys())
    
    lastSave = []        
    #retourner la copie la plus récente
    for key in saves:
        project = client.get_project(key)
        #on suppose que les projets dupliqués n'ont aucune action réalisé dessus après la duplication (pas plus que 100)
        projectTimeline = project.get_timeline(item_count=100)
        projectTimeline = [(item['projectKey'],item['time']) for item in projectTimeline["items"] if item['action']=='PROJECT_DUPLICATE']
        if len(projectTimeline)>0:
            projectTimeline = max(projectTimeline, key=lambda tup: tup[1])
            lastSave.append(projectTimeline)
    return max(lastSave, key=lambda tup: tup[1]) if len(lastSave)>0 else None

# Retourne vrai si aucune sauvegarde d'un projet n'existe ou si elle est obsolète
def shouldISave(projectId,client,logger=None):  
    
    project = client.get_project(projectId)
    
    # On récupère la dernière sauvegarde (si elle existe)
    lastSave = getLastDuplication(projectId,client)
    lastSave = None if lastSave==None or lastSave[0]==projectId else lastSave
    print(lastSave)
    # Si aucune sauvegarde alors on doit sauvegarder 
    if lastSave == None:
        return True
    else:
        cptOp = 0
        #on suppose que la sauvegarde a eu lieu dans les 10000 dernières activités du projet
        timeline = project.get_timeline(item_count=10000)
        for i in timeline['items']:
            if i['time'] > lastSave[1]:
                cptOp += 1
        if cptOp > 0 :
            logger.add_log(severity="INFO",topic="SAUVEGARDE",description="{:} changements ont eu lieu depuis la dernière sauvegarde.".format(cptOp))

    # On sauvegarde uniquement si des changements ont eu lieu depuis la dernière sauvegarde 
    return True if cptOp > 0 else False

# Execute le processus de sauvegarde en vérifiant s'il faut sauvegarder et supprimer des duplications inutiles
def executeSavingProcess(client=None,project=None,is_simulation=None,logger=None):
    projectId = project.get_summary()['projectKey']
    
    backup_folderName="Sauvegardes_pre_normalisation"
    #Génération timestamp et clé
    timestamp=datetime.now().strftime("%y%m%d%H%M%S")
    backupProject='SAVE'+timestamp+"__"+projectId
    
    savingProcessObject = {"shouldISave":None,"backup_project_name":backupProject,"backup_folder_name":backup_folderName,"same_project_duplication_list":[]}
    
    if shouldISave(projectId,client,logger):
        savingProcessObject['shouldISave']=True
        if is_simulation:
            logger.add_log(severity="INFO",topic="SAUVEGARDE",description="MODE SIMULATION : La sauvegarde n'a pas été effectuée.")
        else:
            # on appelle la méthode de sauvegarde
            saving(client=client,project=project,logger=logger)
            logger.add_log(severity="INFO",topic="SAUVEGARDE",description="Succès : La sauvegarde du projet a correctement été effectuée.")

            # on récupère les duplications du projet
            duplications = getProjectDuplications(projectId, client.list_project_keys())
            # on garde uniquement la dernière sauvegarde s'il y en a plusieurs
            if len(duplications)>1 :
                duplicationsWithDate = []
                for key in duplications:
                    project = client.get_project(key)
                    #on suppose que les projets dupliqués n'ont aucune action réalisée dessus après la duplication
                    projectTimeline = project.get_timeline(item_count=100)
                    # on suppose que PROJECT_DUPLICATE n'apparaît qu'une fois pour chaque projet
                    duplicationsWithDate.append([ (item['projectKey'],item['time']) for item in projectTimeline["items"] if item['action']=='PROJECT_DUPLICATE'][0])

                # on trie les sauvegardes par date croissante
                duplicationsWithDate = sorted(duplicationsWithDate, key = lambda x : x[1])
                
                # tous les projets dupliqués SAUF le plus récent
                for i in range(len(duplicationsWithDate)-1):
                    savingProcessObject['same_project_duplication_list'].append(duplicationsWithDate[i][0])
                    logger.add_log(severity="WARNING",topic="SAUVEGARDE",description="Lors de la sauvegarde, d'autres duplications de ce projet ont été identifiées, pensez à les supprimer si elles ne sont plus utiles. ({:})".format(duplicationsWithDate[i][0]))
                    # on supprime les duplications précédentes (peut être pas les droits mais envoyer une notif à l'admin, à minima, pour le notifier de trop de projets dupliqués)                
                    # Supprimer le projet
                    #project = client.get_project(duplicationsWithDate[i][0])
                    #project.delete()
            
    else:
        logger.add_log(severity="INFO",topic="SAUVEGARDE",description="Une sauvegarde de ce projet avec la même version existe déjà. Pas besoin de sauvegarder.")
        savingProcessObject['shouldISave']=False
        
    return savingProcessObject
        
def saving(client=None,project=None,logger=None):
    projectKey = project.get_summary()['projectKey']
    #Génération timestamp et clé
    timestamp=datetime.now().strftime("%y%m%d%H%M%S")
    backupProject='SAVE'+timestamp+"__"+projectKey
    #Dossier rassemblant les sauvegardes
    backup_folderName="Sauvegardes_pre_normalisation"

    #Génération dossier de sauvegarde si inexistant
    if not any(folder.get_name()==backup_folderName for folder in client.get_root_project_folder().list_child_folders()):
        backupFolder=client.get_root_project_folder().create_sub_folder(backup_folderName)
    else:
        for folder in client.get_root_project_folder().list_child_folders():
            if folder.get_name()==backup_folderName:
                backupFolder=folder
                
    try:
        project.duplicate(target_project_key=backupProject, target_project_name='Sauvegarde du '+timestamp+' de '+projectKey, duplication_mode='NONE', export_analysis_models=False, export_saved_models=False, export_git_repository=False, export_insights_data=False, remapping=None, target_project_folder=backupFolder)
        print("Copie "+backupProject+" disponible dans le dossier '"+backup_folderName+"'")
    except:
        raise SaveException("La sauvegarde n'a pas fonctionné. Impossible de continuer la normalisation.")
        
    try:
        #change status of duplication as 'archived'
        project_duplicated = client.get_project(backupProject)
        settings = project_duplicated.get_settings()
        settings.get_raw()['projectStatus'] = 'Archived'
        settings.save()
    except: 
        #afficher un warning si la duplication n'a pas été convertie en archive
        logger.add_log(severity="WARNING",topic="SAUVEGARDE",description="Le status 'archived' n'a pas pu être attribué à la duplicaiton du projet {:}".format(backupProject))