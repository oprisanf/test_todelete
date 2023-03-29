import sdf

def flatten_json(df_arg: SDF, index: int =1) -> SDF:
    #flatten json using recursion
    
    df=update_column_names(df_arg, index) if index ==1 else df_arg
    
    fields=df.schema.fields
    
    for field in fields:
        data_type=str(field.dataType)
        column_name=field.name
        
        first_10_chars= data_type[0:10]
        
        #If it's an Array column
        if first_10_chars == "ArrayType(":
            print("ok")
            #explode array column
            df_temp=df.withColumn(column_name, explode_outer(col(column_name)))
            return flatten_json(df_temp, index + 1)
        
        #If it's a json object
        elif first_10_chars=='StructType':
            current_col=column_name
            
            append_str=current_col
            
            data_type_str=str(df.schema[current_col].dataType)
            
            df_temp=df.withColumnRenamed(column_name, column_name + "#1") if column_name in data_type_str else df
            current_col=current_col + "#1" if column_name in data_type_str else current_col
            
            #expand struct column values
            df_before_expanding = df_temp.select(f"{current_col}.*")
            newly_gen_cols=df_before_expanding.columns
            
            #find next level value for the column
            begin_index =append_str.rfind('*')
            end_index=len(append_str)
            level =append_str[begin_index + 1: end_index]
            next_level = int(level) +1
            
            #Update columns names with new level
            custom_cols =dict((field,f"{append_str}->{field}*{next_level}") for field in newly_gen_cols)
            #custom_cols =dict((field,f"{field}*{next_level}*{field}") for field in newly_gen_cols)
            df_temp2= df_temp.select("*", f"{current_col}.*").drop(current_col)
            df_temp3= df_temp2.transform(lambda df_x:rename_dataframe_cols(df_x,custom_cols))
            
            return flatten_json(df_temp3, index + 1)
        
        
    return df