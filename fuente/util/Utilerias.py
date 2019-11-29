import sys


def obt_ruta(nom_directorio, nom_archivo):
    if sys.platform != 'win32':
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "/" + nom_archivo
    else:
        if nom_directorio == '':
            directorio = nom_archivo
        else:
            directorio = nom_directorio + "\\" + nom_archivo
    return directorio