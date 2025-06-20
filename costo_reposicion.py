import pandas as pd
import numpy as np
import time
from itertools import permutations

class Limpieza:
    @staticmethod
    def creacion_col(archivo_bd, archivo_consulta, archivo_combinaciones):
        start_time = time.time()
        print(f"Tiempo de Inicio: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")


        # Cargar datos
        df = pd.read_csv(archivo_bd, sep='|', encoding='latin1', dtype=str)
        df_c = pd.read_csv(archivo_consulta, sep='\t', dtype=str)
        df_com = pd.read_csv(archivo_combinaciones, sep='\t', dtype=str)

        # Normalizar columnas
        df_com.columns = df_com.columns.str.strip()
        for col in ['CODIGO_USO_PREDOMINANTE', 'CODIGO_USO_01', 'CODIGO_USO_02', 'CODIGO_USO_03']:
            df_com[col] = df_com[col].str.zfill(3)
        df_c['CODIGO_USO'] = df_c['CODIGO_USO'].str.zfill(3)

        # Columnas adicionales
        df['CODIGO_USO_ESTANDAR'] = df['CODIGO_USO'].str.zfill(3)
        df['CONCT_CONTRUCCION_USO'] = df['CLASE_CONSTRUCCION'] + df['CODIGO_USO_ESTANDAR']
        df['CONCT_CONSTRUCCION_UNIDAD_CAL'] = df['CLASE_CONSTRUCCION'] + df['UNIDAD_CALIFICADA']
        df['CANT_UNIDADES_CONTRUC'] = df['CHIP'].map(df['CHIP'].value_counts())
        df['CANT_PRESUPUESTOS'] = df['CANT_UNIDADES_CONTRUC']
        df['GRUPO_USO_PRESUPUESTO_ASOCIADO'] = df['CODIGO_USO'].map(df_c.set_index('CODIGO_USO')['GRUPO_USO_PRESUPUESTO']).fillna('Sin Informacion')

        df['AREA_USO'] = pd.to_numeric(df['AREA_USO'], errors='coerce')
        df['MAX_NUM_PISO'] = pd.to_numeric(df['MAX_NUM_PISO'], errors='coerce')



        # USO_PREDOMINANTE
        df_repetidos = df[df['CANT_PRESUPUESTOS'] > 1]
        idx_max_area = df_repetidos.groupby('CHIP')['AREA_USO'].idxmax()
        idx_max_area = idx_max_area.dropna().astype(int)  # <-- limpieza
        uso_predominante_dict = df.loc[idx_max_area].set_index('CHIP')['CODIGO_USO_ESTANDAR'].to_dict()
        df['USO_PREDOMINANTE'] = df['CHIP'].map(uso_predominante_dict)

        # Presupuesto especial
        condiciones = [
            (df['CODIGO_USO'] == '012') & (df['AREA_USO'] <= 350) & (df['MAX_NUM_PISO'] <= 5),
            (df['CODIGO_USO'] == '012') & ((df['AREA_USO'] > 350) | (df['MAX_NUM_PISO'] > 5)),
            (df['CODIGO_USO'] == '020') & (df['CLASE_CONSTRUCCION'] == 'R'),
            (df['CODIGO_USO'] == '020') & (df['CLASE_CONSTRUCCION'] == 'C'),
            df['CODIGO_USO'].isin(['001', '002', '009']) & df['ARMAZON_ESTRUCTURA'].isin(['111', '112']),
            df['USO_PREDOMINANTE'].isin(['001', '002', '009']) & df['ARMAZON_ESTRUCTURA'].isin(['111', '112'])
        ]
        valores = [
            'PRESUPUESTO RESIDENCIAL',
            'PRESUPUESTO COMERCIAL',
            'PRESUPUESTO RESIDENCIAL',
            'PRESUPUESTO COMERCIAL',
            'PRESUPUESTO INDEPENDIENTE',
            'PRESUPUESTO INDEPENDIENTE'
        ]
        # 2. Crear la columna
        df['PRESU_ESPECIAL'] = np.select(condiciones, valores, default=None)

        # 3. Limpieza 
        df['PRESU_ESPECIAL'] = df['PRESU_ESPECIAL'].fillna('').astype(str).str.strip()

        condiciones_validar = [
            (df['CODIGO_USO'] == '012') & (df['AREA_USO'] <= 350) & (df['MAX_NUM_PISO'] <= 5),
            (df['CODIGO_USO'] == '020') & (df['CLASE_CONSTRUCCION'] == 'R'),
        ]
        valores_validar = [
            'VALIDAR_COMBINACION',
            'VALIDAR_COMBINACION',
        ]
        df['VALIDACION'] = np.select(condiciones_validar, valores_validar, default=None)

        # 3. Limpieza segura
        df['VALIDACION'] = df['VALIDACION'].fillna('').astype(str).str.strip()


        # ARREGLO_COMBINACION y COMBINATORIA
        uso_agrupado = df.groupby('CHIP')['CODIGO_USO_ESTANDAR'].apply(lambda x: sorted(x)).to_dict()
        df['ARREGLO_COMBINACION'] = df['CHIP'].map(uso_agrupado)




        def generar_combinaciones_todas(arreglo):

            if 1 < len(arreglo) < 4:
                return sorted(set(permutations(arreglo)))
            else:
                return [tuple(arreglo)]



        def generar_combinatoria(row):
            arreglo = row.get('ARREGLO_COMBINACION') or []
            
            presu = (row.get('PRESU_ESPECIAL') or '').strip()
            validacion = (row.get('VALIDACION') or '').strip()

            valores_permitidos = [
                'PRESUPUESTO RESIDENCIAL',
                'PRESUPUESTO COMERCIAL',
                'PRESUPUESTO INDEPENDIENTE'
            ]

            caso_1 = not any(valor in presu for valor in valores_permitidos)

            caso_2 = any(valor in presu for valor in valores_permitidos) and validacion == 'VALIDAR_COMBINACION'


            if (caso_1 or caso_2) and len(arreglo) > 1:
                return generar_combinaciones_todas(arreglo) 
            else:
                return []  




        df['COMBINATORIA'] = df.apply(generar_combinatoria, axis=1)







        # Mapa combinaciones
        df_com['LLAVE'] = df_com.apply(
            lambda row: '|'.join([
                str(row['CODIGO_USO_PREDOMINANTE']) if pd.notna(row['CODIGO_USO_PREDOMINANTE']) else '',
                str(row['CODIGO_USO_01']) if pd.notna(row['CODIGO_USO_01']) else '',
                str(row['CODIGO_USO_02']) if pd.notna(row['CODIGO_USO_02']) else '',
                str(row.get('CODIGO_USO_03', '')) if pd.notna(row.get('CODIGO_USO_03', '')) else ''
            ]),
            axis=1
        )
        mapa_combinaciones = df_com.set_index('LLAVE')['GRUPO_USO_PRESUPUESTO'].to_dict()



        # Paso 1: Inicializar columna PRESU
        df['PRESU'] = 'Sin Informacion'

        # Paso 2: Filtrar registros con CANT_PRESUPUESTOS 2 o 3
        df_filtrado = df[df['CANT_PRESUPUESTOS'].isin([2, 3])].copy()

        # Paso 3: Identificar CHIPs con alguna combinatoria vacía
        chips_con_combinatoria_vacia = df_filtrado[df_filtrado['COMBINATORIA'].apply(lambda x: not x)]['CHIP'].unique()

        # Paso 4: Asignar 'Sin Informacion' a todos los registros con esos CHIP
        df.loc[df['CHIP'].isin(chips_con_combinatoria_vacia), 'PRESU'] = 'Sin Informacion'

        # Paso 5: Aplicar lógica solo a los CHIP válidos (que NO tengan combinatoria vacía)
        for chip, grupo in df_filtrado.groupby('CHIP'):
            if chip in chips_con_combinatoria_vacia:
                continue  # Saltar si ya tiene combinatoria vacía

            uso_pred = grupo['USO_PREDOMINANTE'].iloc[0]
            combinatorias = grupo['COMBINATORIA'].iloc[0]
            if pd.isna(uso_pred):
                continue
            uso_pred = uso_pred.zfill(3)
            grupo_resultado = 'Sin Informacion'
            for perm in combinatorias:
                perm_zfill = [x.zfill(3) for x in perm]
                if len(perm_zfill) >= 4:
                    grupo_resultado = "Presupuesto >= 4"
                    break
                llave = '|'.join([uso_pred] + perm_zfill + [''] * (3 - len(perm_zfill)))
                resultado = mapa_combinaciones.get(llave)
                if resultado:
                    grupo_resultado = resultado
                    break
            df.loc[df['CHIP'] == chip, 'PRESU'] = grupo_resultado



        

        # Presupuesto final
        def calcular_presupuesto_final(row):
            if row['PRESU_ESPECIAL'] in ['PRESUPUESTO RESIDENCIAL', 'PRESUPUESTO COMERCIAL']:
                return row['PRESU_ESPECIAL']
            elif row['PRESU_ESPECIAL'] == 'PRESUPUESTO INDEPENDIENTE':
                return row['GRUPO_USO_PRESUPUESTO_ASOCIADO']
            elif row['CANT_PRESUPUESTOS'] == 1:
                return row['GRUPO_USO_PRESUPUESTO_ASOCIADO']
            elif row['CANT_PRESUPUESTOS'] in [2, 3]:
                return row['PRESU'] if row['PRESU'] != "Sin Informacion" else row['GRUPO_USO_PRESUPUESTO_ASOCIADO']
            elif row['CANT_PRESUPUESTOS'] >= 4:
                return f"{row['GRUPO_USO_PRESUPUESTO_ASOCIADO']}"
            return None

        df['PRESUPUESTO_FINAL'] = df.apply(calcular_presupuesto_final, axis=1)

        # Tipo de presupuesto
        def determinar_tipo_presupuesto(row):
            if row['PRESU'] == 'Sin Informacion':
                return 'PRESUPUESTO INDEPENDIENTE'
            elif row['PRESU_ESPECIAL'] == 'PRESUPUESTO INDEPENDIENTE':
                return 'PRESUPUESTO INDEPENDIENTE'
            elif row['PRESUPUESTO_FINAL'] == 'Sin Informacion':
                return 'Sin Informacion'
            elif row['PRESU'] in ['PRESUPUESTO COMERCIAL', 'PRESUPUESTO RESIDENCIAL']:
                return 'PRESUPUESTO UNICO'
            elif row['PRESU_ESPECIAL'] in ['PRESUPUESTO INDEPENDIENTE', 'PRESUPUESTO RESIDENCIAL', 'PRESUPUESTO COMERCIAL'] and row['CANT_PRESUPUESTOS'] == 1:
                return 'PRESUPUESTO INDEPENDIENTE'
            else:
                return 'Sin Informacion'

        df['TIPO_PRESUPUESTO'] = df.apply(determinar_tipo_presupuesto, axis=1)
        # Agregar la columna "ES_USO_PREDOMINANTE"
        df['ES_USO_PREDOMINANTE'] = 'NO'  
        df.loc[idx_max_area, 'ES_USO_PREDOMINANTE'] = 'SI'  


        # Presupuesto Independiente2
        chips_independientes = df[df['PRESU_ESPECIAL'] == 'PRESUPUESTO INDEPENDIENTE']['CHIP'].unique()


        df.loc[df['CHIP'].isin(chips_independientes), 'TIPO_PRESUPUESTO'] = 'PRESUPUESTO INDEPENDIENTE'


        # Guardado de resultados
        df.to_csv(f"resultado_puntos_oferta.txt", sep='|', encoding='latin1', index=False)

        max_filas_excel = 800000
        if len(df) <= max_filas_excel:

            df.to_excel(f"resultado_puntos_oferta.xlsx", index=False)
        else:

            with pd.ExcelWriter(f"resultado_puntos_oferta.xlsx", engine='xlsxwriter') as writer:
                for i in range(0, len(df), max_filas_excel):
                    df.iloc[i:i + max_filas_excel].to_excel(writer, sheet_name=f'Sheet{i // max_filas_excel + 1}', index=False)





        print(f"Tiempo de ejecución: {time.time() - start_time:.2f} segundos")
        print("FIN::::--------::::")
