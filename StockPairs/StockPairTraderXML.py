# Zdeněk Valečko
# 2020-06-09
# Skript pro práci s XML pro Stock Pair Trader / Backtester

import lxml.etree as et

MaxPositions=3
FittingModel="LinReg"
WalkForward=6 

Model="TStockPair_Ratio"
Period=15
TimeSL=15
EntryLevel=2
ExitLevel=0


def ExportInputsToXml(xml_file_path, inputs, start_date_str, end_date_str):
    root = et.Element("Root")
    
    # uzel Settings
    settings_node = et.SubElement(root, "Settings")
    settings_node.set("MaxPositions", str(MaxPositions) )
    settings_node.set("TestFrom", start_date_str )
    settings_node.set("TestTo", end_date_str )
    settings_node.set("FittingModel", FittingModel )
    settings_node.set("WalkForward", str(WalkForward) )
    
    # uzel Model
    model_node = et.SubElement(root, "Model")
    model_node.set("Model", Model )
    model_node.set("Period", str(Period) )
    model_node.set("TimeSL", str(TimeSL) )
    model_node.set("EntryLevel", str(EntryLevel) )
    model_node.set("ExitLevel", str(ExitLevel) )
    
    # uzly Inputs
    inputs_node = et.SubElement(root, "Inputs") 
    for input_for_date in inputs:
        month_start_date = input_for_date[0]
        month_end_date = input_for_date[1]
        sectors_for_date = input_for_date[2]
    
        input_node = et.SubElement(inputs_node, "Input")
        input_node.set("ValidFrom", '{:%Y-%m-%d}'.format(month_start_date) )
        input_node.set("ValidTo", '{:%Y-%m-%d}'.format(month_end_date) )
        sectors_node = et.SubElement(input_node, "Sectors")
        for sector_name, stocks_in_sector in sectors_for_date.items():
            sector_node = et.SubElement(sectors_node, "Sector")
            sector_node.set("Name", sector_name )
            sector_node.text = stocks_in_sector
        
    # konstrukce stromu
    tree = et.ElementTree(root)
    # zapis do souboru
    tree.write(xml_file_path)

