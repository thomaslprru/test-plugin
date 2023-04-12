from autodus.process import normalisation_tbl
import pandas as pd

#Fonction permettant de normaliser les recettes SQL d'un flow
def normaliseSQLRecipe(project=None,connexions_dict=None,is_simulation=None,env_variables=None,logger=None):
    normaliseSQLRecipe = {"nb_connexion":0,"nb_connexion_normalise":0,"dataframe_html":None}
    columnName = {"recette":"Recette SQL","ori":"Avant modification","mod":"Après modification"}
    recipeControlDf = pd.DataFrame(columns=[columnName['recette'],columnName['ori'],columnName['mod']])

    #Parcours des recettes SQL
    for recipeItem in project.list_recipes():
        if recipeItem.type=="sql_query" or recipeItem.type=="sql_script":
            recipe = project.get_recipe(recipeItem.name)
            settings = recipe.get_settings()
            sqlQuery = settings.get_payload()
            
            #todo : validate query before ??
            
            #normalisation avec la variable TBL
            updatedSqlQuery,before_after_normalization = normalisation_tbl.normaliseWithTBL(text=sqlQuery,setting=settings,connection_dict=connexions_dict,
                                                                 name=recipeItem.name,project=project,env_variables=env_variables,logger=logger)
            
            onlyDifferenceOriHTML = ' '.join(["{:} <br/><br/>".format(txt[0]) for txt in before_after_normalization])
            onlyDifferenceModHTML = ' '.join(["{:} <br/><br/>".format(txt[1]) for txt in before_after_normalization])

            recipeControlDf=recipeControlDf.append({columnName['recette']: recipe.name,columnName['ori'] :  onlyDifferenceOriHTML, columnName['mod'] : onlyDifferenceModHTML, 'Difference': 'No' if not len(before_after_normalization) else 'Yes'}, ignore_index=True)
            
            #Si pas de simulation, enregister
            if not is_simulation:
                logger.raiseErrors()
                settings.set_payload(updatedSqlQuery)
                settings.save()

    #Afficher le bilan AVANT/APRES
    if len(recipeControlDf)>0:
        normaliseSQLRecipe['nb_connexion'] = len(recipeControlDf)
        recipeControlDf = recipeControlDf[recipeControlDf['Difference']=="Yes"]
        recipeControlDf = recipeControlDf.drop(['Difference'], axis=1)
        normaliseSQLRecipe['nb_connexion_normalise'] = len(recipeControlDf)
        #Si des différences sont présentes entre avant/après normalisation
        if len(recipeControlDf)>0:
            normaliseSQLRecipe['dataframe_html'] = recipeControlDf
         
    return normaliseSQLRecipe