#!/usr/bin/env python

"""
Merges a MySQL dump data and a PgSQL dump data with the right format so it can be directly
imported to a new PostgreSQL database.

Data Dump using:
mysqldump -t --compatible=postgresql --default-character-set=utf8 -uroot -p gitlabhq_production -r mysql_gitlabhq_production_dataonly.mysql
pg_dump -a -U postgres gitlabhq_production -f postgres_gitlabhq_production_dataonly.dump

DDL Dump using:
pg_dump -s -U postgres gitlabhq_production -f postgres_gitlabhq_production_ddlonly.dump

Eg:
# Execcute this script using:
db_data_merge.py mysql_gitlabhq_production_dataonly.mysql postgres_gitlabhq_production_ddlonly.dump gitlabhq_production_merged.psql

# PostgreSQL Restore
psql -U postgres -d gitlabhq_production -f gitlabhq_production_merged.psql
"""

import sys
import os
import time
import subprocess


def parse(data_input_filename, ddl_input_filename, output_filename):
    
    # Check lines count
    num_lines = -1
    if ddl_input_filename == "-":
        ddl_num_lines = -1
    else:
        ddl_num_lines = int(subprocess.check_output(["wc", "-l", ddl_input_filename]).strip().split()[0])
    
    if data_input_filename == "-":
        data_num_lines = -1
    else:
        data_num_lines = int(subprocess.check_output(["wc", "-l", data_input_filename]).strip().split()[0])

    num_lines = ddl_num_lines + data_num_lines
    
    started = time.time()
    
    # Open output file and write header. Logging file handle will be stdout
    # unless we're writing output to stdout, in which case NO PROGRESS FOR YOU.
    if output_filename == "-":
        output = sys.stdout
        logging = open(os.devnull, "w")
    else:
        output = open(output_filename, "w")
        logging = sys.stdout

    # Write data into temporary output file
    data_tables = {}
    data_num_inserts = 0
    if data_input_filename == "-":
        data_input_fh = sys.stdin
    else:
        data_input_fh = open(data_input_filename)
    temp_output_filename = "data_output.tmp"
    tmp_output = open(temp_output_filename, "w")
    for d, data_line in enumerate(data_input_fh):
        time_taken = time.time() - started
        percentage_done = (d + 1) / float(data_num_lines)
        secs_left = (time_taken / percentage_done) - time_taken
        write_log = "\rCollect Inserts: Line %i (of %s: %.2f%%) [%s tables] [%s inserts] [ETA: %i min %i sec]" % (
            d + 1,
            data_num_lines,
            ((d + 1) / float(data_num_lines)) * 100,
            len(data_tables),
            data_num_inserts,
            secs_left // 60,
            secs_left % 60,
        )
        logging.write(write_log)
        logging.flush()

        data_line = data_line.decode("utf8").strip()\
            .replace(r"\\", "#WUBWUBREALSLASHWUB#")\
            .replace(r"\'", "''")\
            .replace("#WUBWUBREALSLASHWUB#", r"\\")
        # Ignore comment lines
        if data_line.startswith("--") \
                or data_line.startswith("/*") \
                or data_line.startswith("LOCK TABLES") \
                or data_line.startswith("DROP TABLE") or data_line.startswith("UNLOCK TABLES") or not data_line:
            continue
        if data_line.startswith("INSERT INTO"):
            current_data_table = data_line.split('"')[1]
            data_tables[current_data_table] = current_data_table
            # tmp_output.write(data_line.encode("utf8").replace("'0000-00-00 00:00:00'", "NULL") + "\n")
            tmp_output.write(data_line.encode("utf8") + "\n")
            data_num_inserts += 1
    tmp_output.close()
    
    # DDL Process
    # Variables
    ddl_tables = {}
    ddl_sequences = {}
    
    create_current_table = None
    create_current_sequence = None
    create_table_lines = []
    create_sequence_lines = []
    
    # Alter Columns
    alter_for_mysql_insert_lines = []
    cast_columns_back_lines = []
    
    alter_owner_obj = None
    alter_owner_lines = []
    
    alter_current_table = ""
    alter_default_sequence_lines = []
    alter_sequences = {}
    
    alter_constraint_lines = []
    
    index_line = ""
    index_lines = []
    
    if ddl_input_filename == "-":
        ddl_input_fh = sys.stdin
    else:
        ddl_input_fh = open(ddl_input_filename)

    output.write("-- Converted by db_converter\n")
    output.write("START TRANSACTION;\n")
    print write_log

    # DDL Process
    direct_output_flag = False
    
    started = time.time()
    for i, line in enumerate(ddl_input_fh):
        time_taken = time.time() - started
        percentage_done = (i+1) / float(ddl_num_lines)
        secs_left = (time_taken / percentage_done) - time_taken
        logging.write("\rMerge Dump: Line %i (of %s: %.2f%%) [ETA: %i min %i sec]" % (
            i + 1,
            ddl_num_lines,
            ((i+1)/float(ddl_num_lines))*100,
            secs_left // 60,
            secs_left % 60,
        ))
        logging.flush()
        line = line.decode("utf8").strip().replace(r"\\", "WUBWUBREALSLASHWUB").replace(r"\'", "''").replace("WUBWUBREALSLASHWUB", r"\\")
        # Ignore comment lines
        if line.startswith("--") or line.startswith("/*") or line.startswith("LOCK TABLES") or line.startswith("DROP TABLE") or line.startswith("UNLOCK TABLES") or not line:
            continue
            
        if direct_output_flag or line.startswith("CREATE EXTENSION ") or line.startswith("SET ") or line.startswith("COMMENT ON EXTENSION"):
            direct_output_flag = True
            if line.endswith(";"):
                direct_output_flag = False
            output.write("%s\n" % line)
            
        elif create_current_table is not None or line.startswith("CREATE TABLE "):
            if create_current_table is None:
                create_current_table = line.split(' ')[2]
                ddl_tables[create_current_table] = create_current_table
            if line.endswith(";"):
                create_current_table = None
            create_table_lines.append("%s\n" % line)
            
            # Cast boolean
            if line.find(" boolean") > -1:
                current_column = line.replace(",", " ").split(" boolean")[0]
                
                cast_columns_back_lines.append("ALTER TABLE %s ALTER COLUMN %s type boolean USING (%s::bool)" % (create_current_table, current_column, current_column))
                if line.find(" DEFAULT ") > -1:
                    default_value = line.replace(",", " ").split(" DEFAULT ")[1].split(" ")[0]
                    alter_for_mysql_insert_lines.append("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT" % (create_current_table, current_column))
                    cast_columns_back_lines.append("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s" % (create_current_table, current_column, default_value))
                if line.find("NOT NULL") > -1:
                    cast_columns_back_lines.append("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL" % (create_current_table, current_column))
                alter_for_mysql_insert_lines.append("ALTER TABLE %s ALTER COLUMN %s type integer USING (%s::int4)" % (create_current_table, current_column, current_column))
            
        elif create_current_sequence is not None or line.startswith("CREATE SEQUENCE "):
            if create_current_sequence is None:
                create_current_sequence = line.split(' ')[2]
                ddl_sequences[create_current_sequence] = create_current_sequence
            if line.endswith(";"):
                create_current_sequence = None
            create_sequence_lines.append("%s\n" % line)
            
        elif alter_owner_obj is not None or (line.startswith("ALTER ") and line.find(" OWNER ") > -1):
            if alter_owner_obj is None:
                alter_owner_obj = line.split(' ')[2]
            if line.endswith(";"):
                alter_owner_obj = None
            alter_owner_lines.append("%s\n" % line)
        elif alter_current_table != "" or line.startswith("ALTER TABLE "):
            if not line.endswith(";"):
                alter_current_table += " " + line
                continue
            else:
                alter_current_table += " " + line
                alter_current_table = alter_current_table.lstrip().rstrip()
                #Sequence Default
                if alter_current_table.find(" ALTER COLUMN ") > -1 and alter_current_table.find(" SET DEFAULT ") > -1:
                    tmp_table = alter_current_table.split(" ALTER COLUMN ")[0].split(" ")[-1]
                    tmp_seq_id = alter_current_table.split("'")[1].split("'")[0]
                    tmp_column_id = alter_current_table.split(" ALTER COLUMN ")[1].split(" ")[0]
                    alter_sequences[tmp_seq_id] = (tmp_table, tmp_column_id)
                    alter_default_sequence_lines.append("%s\n" % line)
                # Constraint
                if alter_current_table.find(" ADD CONSTRAINT ") > -1:
                    alter_constraint_lines.append("%s\n" % alter_current_table)
                alter_current_table = ""
                
        elif index_line != "" or line.startswith("CREATE INDEX ") or line.startswith("CREATE UNIQUE INDEX "):
            if not line.endswith(";"):
                index_line += " " + line
                continue
            else:
                index_line += " " + line
                index_line = index_line.lstrip().rstrip()
                # Indexes
                index_lines.append("%s\n" % index_line)
                index_line = ""
            
        
    # Finish file
    output.write("\n-- Post-data save --\n")
    output.write("COMMIT;\n")
    output.write("START TRANSACTION;\n")

    # Write creation
    output.write("\n-- Create Tables --\n")
    for line in create_table_lines:
        output.write("%s" % line)

    output.write("\n-- Create Sequnces --\n")
    for line in create_sequence_lines:
        output.write("%s" % line)

    output.write("\n-- Alter Owner --\n")
    for line in alter_owner_lines:
        output.write("%s" % line)

    output.write("\n-- Alter Default Sequence --\n")
    for line in alter_default_sequence_lines:
        output.write("%s" % line)

    output.write("\n-- Alter Columns For Insert --\n")
    for line in alter_for_mysql_insert_lines:
        output.write("%s;\n" % line)
    
    output.write("\nCOMMIT;\n")
    output.write("\nSTART TRANSACTION;\n")
    
    output.write("\n-- Insert Values --\n")
    tmp_data_fh = open(temp_output_filename)
    for t, line in enumerate(tmp_data_fh):
        output.write(line + "\n")
    tmp_data_fh.close()
    os.remove(os.getcwd() + os.sep + temp_output_filename)

    output.write("\nCOMMIT;\n")
    output.write("\nSTART TRANSACTION;\n")
    
    output.write("\n-- Cast Columns --\n")
    for line in cast_columns_back_lines:
        output.write("%s;\n" % line)
        
    output.write("\n-- Reset Sequence To Max --\n")
    for key in alter_sequences:
        output.write("SELECT setval('%s', max(%s)) FROM %s;\n" % (key, alter_sequences.get(key)[1], alter_sequences.get(key)[0]))

    output.write("\n-- Add Constraints --\n")
    for line in alter_constraint_lines:
        output.write("%s" % line)

    output.write("\n-- Add Indexes --\n")
    for line in index_lines:
        output.write("%s" % line)

    # Finish file
    output.write("\nCOMMIT;\n")

    data_input_fh.close()
    ddl_input_fh.close()
    output.close()
    
    print "Finished"


if __name__ == "__main__":
    # db_data_merge.py mysql_gitlabhq_production_dataonly.mysql postgres_gitlabhq_production_ddlonly.dump gitlabhq_production_merged.psql
    parse(sys.argv[1], sys.argv[2], sys.argv[3])
