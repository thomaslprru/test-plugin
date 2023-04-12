# Ce fichier gère l'affichage de sortie de la macro de variabilisation

# Permet d'exporter le code HTML d'un bouton
def createButton(id=None):
    return """<div class="open-btn">
      <button class="open-button" onclick="openForm('"""+id+"""')"><strong>Voir</strong></button>
    </div>"""

# Permet d'exporter le code HTML d'une popup
def createPopup(id=None,title=None,table=None):
    return """
    <div class="login-popup">
      <div class="form-popup" id='"""+ id +"""'>
        <form class="form-container">
          <h2>"""+title+"""</h2>
          """+table+"""
          <button type="button" class="btn cancel" onclick="closeForm('"""+id+"""')">Fermer</button>
        </form>
      </div>
    </div>"""

# Permet de générer le code HTML d'un tableau
def createTable(columns=[],rows=[]):
    htmlCol = "<tr>"
    for col in columns:
        htmlCol += "<th>{:}</th>".format(col)
    htmlCol += "</tr>"
    
    htmlRow = ""
    for r in rows:
        htmlRow += "<tr>"
        for val in r:
            htmlRow += "<td>{:}</td>".format(val)
        htmlRow += "</tr>"
    
    htmlToReturn = """
            <table>
            <thead>
                
                       """ + htmlCol+"""
            </thead>
            <tbody>
                """+ htmlRow +"""
            </tbody>
        </table>
        """
    return htmlToReturn
    

# Génère le code HTML de la sortie de la macro
def resultsToHtml(results,logger):
    popup = []
    
    # Définition de l'en-tête (style)
    html = """<head><style type="text/css">

                table {
                border-collapse: collapse;
                margin: 25px 0;
                font-size: 0.9em;
                font-family: sans-serif;
                min-width: 400px;
                box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
                width:100%;
                }
                
                table thead th {
                    text-align: center;
                    background-color: black;
                    color: white;
                }

                td, th {
                border: 2px #2b2b2b solid;
                padding: 12px 15px;
                }
                
                tbody tr {
                    border-bottom: 1px solid #dddddd;
                }
                
                
                tbody tr:last-of-type {
                    border-bottom: 2px solid #009879;
                }

                
                nav{
                    width: 100%;
                    margin: 0px auto 40px auto;
                    background-color: white;
                    position: sticky;
                    top: 0px;
                }

                nav ul{
                    list-style-type: none;
                }

                nav li{
                    float: left;
                    width: 25%;/*100% divisé par le nombre d'éléments de menu*/
                    text-align: center;/*Centre le texte dans les éléments de menu*/
                }

                /*Evite que le menu n'ait une hauteur nulle*/
                nav ul::after{
                    content: "";
                    display: table;
                    clear: both;
                }

                nav a{
                    display: block; /*Toute la surface sera cliquable*/
                    text-decoration: none;
                    color: black;
                    border-bottom: 2px solid transparent;/*Evite le décalage des éléments sous le menu à cause de la bordure en :hover*/
                    padding: 10px 0px;/*Agrandit le menu et espace la bordure du texte*/
                }

                nav a:hover{
                    color: orange;
                    border-bottom: 2px solid gold;
                }
                
                .selected{
                    color: orange;
                    border-bottom: 2px solid gold;
                }

                .conteneur{
                  margin: 0px 20px;
                  height: 1500px;
                }
                
                .warning{
                    background-color: orange;
                }
                
                /* Positionnez la forme Popup */
                  .login-popup {
                    position: relative;
                    text-align: center;
                    width: 100%;
                  }

                  /* Masquez la forme Popup */
                  .form-popup {
                    display: none;
                    position: fixed;
                    left: 45%;
                    top: 5%;
                    transform: translate(-45%, 5%);
                    border: 2px solid #666;
                    z-index: 9;
                  }
                  /* Styles pour le conteneur de la forme */
                  .form-container {
                    max-width: 700px;
                    padding: 20px;
                    background-color: #fff;
                    margin-bottom: 0px;
                  }

                  /* Stylez le bouton de connexion */
                  .form-container .btn {
                    background-color: #8ebf42;
                    color: #fff;
                    padding: 12px 20px;
                    border: none;
                    cursor: pointer;
                    width: 100%;
                    margin-bottom: 10px;
                    opacity: 0.8;
                  }
                  /* Stylez le bouton pour annuler */
                  .form-container .cancel {
                    background-color: #cc0000;
                  }
                  /* Planez les effets pour les boutons */
                  .form-container .btn:hover,
                  .open-button:hover {
                    opacity: 1;
                  }

                </style></head>"""
    
    
    if results['isSimulation']:
        html+="<h3>Mode simulation : aucun effet sur le flow.</h3><h5>Après vérification de la sortie de votre part, vous pouvez réexécuter la macro sans le mode simulation.</h5>"
    else:
        html+="<h3>Mode normalisation : le projet a été normalisé.</h3>"
        
    logsCpt = logger.get_nb_log()
    
    logsVal="({:})".format(logsCpt['all']) if logsCpt['all']>0 else ""
    
    # Création du menu de la sortie 
    html += """<body> 
         <nav>
          <ul>
            <li><a id="generalList" onClick="changeStep('general');">Général</a></li>
            <li><a id="logsList" onClick="changeStep('logs');">Logs """+ logsVal +"""</a></li>
            <li><a id="recetteSqlList" onClick="changeStep('recettesql');">Recettes SQL</a></li>
            <li><a id="connexionBqList" onClick="changeStep('connexionbq');">Connexions BigQuery</a></li>
          </ul>
        </nav>
    """
    
    # Rubrique générale
    html += """<div id="general">"""
    
    
    html+= """<table border="1" class="dataframe">

  <tbody>"""
    if "sql_recipe_normalization" in results:
        html+= """
        <tr>
          <td>Recettes SQL Normalisés</td>
          <td>"""+ str(results['sql_recipe_normalization']['nb_connexion_normalise']) +"/"+ str(results['sql_recipe_normalization']['nb_connexion']) +"""</td>
          <td></td>
        </tr>
        """
    if "connexion_normalization" in results:
        html+="""
            <tr>
              <td>Connexions BQ Normalisés</td>
              <td>""" + str(results['connexion_normalization']['nb_connexion_normalise'])+ "/"+ str(results['connexion_normalization']['nb_connexion']) +"""</td>
              <td></td>
            </tr>
            """
    saveValue = "OUI" if results['save']['shouldISave'] else "NON"
    savePathValue = results['save']['backup_folder_name']+"/"+results['save']['backup_project_name']
    html+="""
	<tr>
      <td>Projet nécessite une sauvegarde</td>
      <td>""" + saveValue + """</td>
      <td></td>
    </tr>"""
    
    if results['save']['shouldISave']:
        html+="""
            <tr>
              <td>Adresse de la sauvegarde</td>
              <td>"""+ savePathValue +"""</td>
              <td></td>
            </tr>
            <tr>"""
    
    if len(results['shared_datasets'])>0:
        sharedDSValue = "OUI (" + str(len(results['shared_datasets'])) + ")"  
        showButton = createButton(id="sharedPopup")
        popup.append(createPopup(id="sharedPopup",title="Shared dataset",table=createTable(columns=["Type","Nom","Projet"], rows=[[v['type'], v['name'], v['projectKey']] for v in results['shared_datasets']] )))
    else:
        sharedDSValue = "NON"
        showButton = ""
    
    html+="""
      <td>Projet contient SharedDataset</td>
      <td>"""+sharedDSValue+"""</td>
      <td>"""+ showButton +"""</td>
    </tr>"""
    
    if results['connexion_python_recipe'] is not None:
        if len(results['connexion_python_recipe'])>0:
            connexionPythonRecipeValue = "OUI (" + str(len(results['connexion_python_recipe'])) + ")"  
            connexionPythonRecipeClass = "warning"
            showButton = createButton(id="pythonConnexionRecipe")
            popup.append(createPopup(id="pythonConnexionRecipe",title="Connexion recette Python",table=createTable(columns=["Nom recette"], rows=[[v['name']] for v in results['connexion_python_recipe']])))
        else:
            connexionPythonRecipeValue = "NON"
            connexionPythonRecipeClass = ""
            showButton = ""

        html+="""
        <tr class='"""+ connexionPythonRecipeClass +"""'>
          <td>Connexion dans des recettes Python</td>
          <td>"""+connexionPythonRecipeValue+"""</td>
          <td>"""+showButton+"""</td>
        </tr> """
    
    if len(results['shouldBeIndustrialized'])>0:
        shouldBeIndustrializedValue = "OUI (" + str(len(results['shouldBeIndustrialized'])) + ")"  
        shouldBeIndustrializedClass = "warning"
        showButton = createButton(id="shouldBeIndustrialized")
        popup.append(createPopup(id="shouldBeIndustrialized",title="Table à industrialiser",table=createTable(columns=["Connexion"], rows=[[v] for v in results['shouldBeIndustrialized']])))
    else:
        shouldBeIndustrializedValue = "NON"
        shouldBeIndustrializedClass = ""
        showButton = ""
        
    html += """
	<tr class='"""+ shouldBeIndustrializedClass +"""'>
      <td>Tables devant être industrialisées</td>
      <td>"""+shouldBeIndustrializedValue+"""</td>
      <td>"""+showButton+"""</td>
    </tr>"""
    
    if len(results['save']['same_project_duplication_list'])>0: 
        showButton = createButton(id="same_project_duplication_list")
        popup.append(createPopup(id="same_project_duplication_list",title="Duplication(s) existante(s)",table=createTable(columns=["Nom du projet"], rows=[[v] for v in results['save']['same_project_duplication_list']])))        
        
        html+="""
        <tr class="warning">
          <td>Duplication du projet à éventuellement supprimer</td>
          <td>OUI</td>
          <td>"""+ showButton +"""</td>
        </tr>"""
    
    html+= """</tbody></table>"""
    
            
    html += """</div>"""
    
    # Rubrique recette SQL
    if "sql_recipe_normalization" in results:
        html += """<div id="recettesql">"""
        if results['sql_recipe_normalization']['dataframe_html'] is not None:
            colonnes = results['sql_recipe_normalization']['dataframe_html'].columns
            html+=createTable(columns = colonnes , rows=[[row[i] for i in colonnes] for index, row in results['sql_recipe_normalization']['dataframe_html'].iterrows()])
        else:
            html+="<div>Aucun changement</div>"
        html+="</div>"
    
    # Rubrique connexions BigQuery
    if "connexion_normalization" in results:
        html += """<div id="connexionbq">"""
        if results['connexion_normalization']['dataframe_html'] is not None:
            html+=results['connexion_normalization']['dataframe_html']

        else:
            html+="<div>Aucun changement</div>"
        html+="</div>"
    for pop in popup:
        html += pop
    
    # Rubrique des logs
    html += """<div id="logs">"""
    html += logger.df_to_html()
    html += """</div>"""
    
    # Définition du code JavaScript
    html += """<script>
                function openForm(id) {
					document.getElementById(id).style.display = "block";
				  }

				  function closeForm(id) {
					document.getElementById(id).style.display = "none";
				  }
                  
				changeStep("general")
				function changeStep(val) {
					rubrique = ["general","recettesql","connexionbq","logs"]
					rubriqueCss = ["generalList","recetteSqlList","connexionBqList","logsList"]
					for(let i=0;i<rubrique.length;i++){
						document.getElementById(rubrique[i]).style.display = 'none';
						document.getElementById(rubriqueCss[i]).className = '';

						if(rubrique[i] === val){
							document.getElementById(rubrique[i]).style.display = 'block';
							document.getElementById(rubriqueCss[i]).className = 'selected';
						}
					}
				}
				</script></body>"""
    return html