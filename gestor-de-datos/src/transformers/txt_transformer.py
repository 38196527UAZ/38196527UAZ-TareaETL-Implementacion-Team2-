##!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------
# Archivo: txt_transformer.py
# Capitulo: Flujo de Datos
# Autor(es): Alexis Tadeo Sanchez Arguello & Adolfo Andres Hernandez Romero & Juan Daniel Sanchez Macias & Eloy Valdez Muruato
# Version: 1.0.0 Abril 2024
# Descripción:
#
#   Este archivo define un procesador de datos que se encarga de transformar
#   y formatear el contenido de un archivo TXT
#-------------------------------------------------------------------------
from src.extractors.txt_extractor import TXTExtractor
from os.path import join
import luigi, os, json

class TXTTransformer(luigi.Task):
    def requires(self):
        return TXTExtractor()

    def run(self):
        result = []    
        for input_file in self.input():
            with input_file.open('r') as txt_file:
                next(txt_file)
                content = txt_file.read()
                # Divide el contenido del archivo por punto y coma para obtener los registros individuales
                records = content.split(';')
                for record in records:
                    # Divide cada registro en campos utilizando la coma como separador
                    fields = [field.strip() for field in record.strip().split(',')]
                    # Añade los campos a la lista de resultados
                    if len(fields) >= 8:
                        result.append({
                            "description": fields[2],
                            "quantity": int(fields[3]),
                            "price": float(fields[5]),
                            "total": int(fields[3]) * float(fields[5]),
                            "invoice": fields[0],
                            "provider": fields[6],
                            "country": fields[7]
                        })

        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = os.path.join(project_dir, "result")
        return luigi.LocalTarget(os.path.join(result_dir, "txt.json"))
