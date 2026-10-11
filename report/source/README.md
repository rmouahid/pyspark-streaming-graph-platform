# Sources du rapport technique

- `rapport_technique.html` : texte du rapport (mise en page A4 en CSS).
- `fig/architecture.png` : schéma d'architecture, généré depuis `architecture.mmd` (Mermaid).
- `fig/dashboard.png` : capture du dashboard pendant une exécution réelle de `run.sh`.

Pour régénérer le PDF, ouvrir `rapport_technique.html` dans Chrome ou Chromium,
puis « Imprimer » → « Enregistrer au format PDF », format A4, avec les
graphiques d'arrière-plan. Le schéma se régénère avec
[mermaid-cli](https://github.com/mermaid-js/mermaid-cli) :

```bash
npx @mermaid-js/mermaid-cli -i architecture.mmd -o fig/architecture.png -b white -s 2
```
