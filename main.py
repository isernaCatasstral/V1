from costo_reposicion import Limpieza




#bd_file = 'BD_Prueba.txt'
#bd_file = 'PUNTOS_MUESTRAL.txt'
bd_file = 'PUNTOS_OFERTA.txt'
#bd_file = 'bloque_3.txt'
#bd_file = 'BD_NPH_OTC 250127.txt'
#bd_file = 'BD_NPH_OTC2.txt'
get_usos_clasification = 'CLASIFICACION_USOS.txt'
get_combinaciones = 'COMBINACIONES_USOS.txt'


def main():
    # Crear una instancia de la clase Limpieza
    limpieza = Limpieza()


    #---Llamar al método creacion_col usando la instancia
    limpieza.creacion_col(bd_file, get_usos_clasification, get_combinaciones)
    

    print("Finalizado***")



if __name__ == "__main__":
    main()