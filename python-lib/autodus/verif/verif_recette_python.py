# Appel connexion dans des recettes Python --------------------------------------------------------------

# Vérif appel Bigquery dans les recettes Python
#Fonction qui affiche un warning si une recette python dans un projet contient le terme bigquery
#!! Peut également retourner vrai alors que le mot est dans un commentaire !!
def isBigqueryCalledInsidePythonRecipe(project=None):
    warningList = []
    forbiddenWordsInRecipe = ['bigquery',"psycopg2"]
    for recipeItem in project.list_recipes(as_type='objects'):
        recipe = recipeItem.get_settings()
        if recipe.type == 'python' :
            for forbiddenWord in forbiddenWordsInRecipe:
                if recipe.get_payload() is not None and forbiddenWord in recipe.get_payload():
                    warningList.append({"name":recipeItem.name})
    return warningList 