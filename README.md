This project uses open data street tree inventories to examine street tree taxonomic diversity and the dominance of native tree species across Canada.
There are four main files: 
1. main.py -> processes the street tree inventories and cleans the botanical names
2. analyzer.py -> calculates native status, assigns downtown versus non-downtown areas, and calculates the diversity indices
3. figures.py -> creates figures used in the study.
4. POWO.py -> uses the Royal Botanic Gardens, KEW Plants of the World Online (POWO) API to verify that the species, genera, and families are accepted. All genera and families are accepted in the current study, but hybrids and cultivars may not be known to POWO.

Each file generates datasets that are then used in other main python files. These datasets are not included in this repository and must be created by implementing the code.
1. Master df.csv -> the results of the main.py file that are used in the analyzer.py file.
