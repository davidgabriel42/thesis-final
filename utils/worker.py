import os
import sys
import pandas as pd
import numpy as np
import re

print('begin feature scan')

#path = './parsed/bwjenkin_ior_id10000121_6-3-28809-3470933701207083758_1.darshan'

path = sys.argv[1]
out_path = path + "-completed.csv"

features_scan = pd.DataFrame()

i = 0

r_nprocs = re.compile(r'nprocs: (\d+)')
r_jobid = re.compile(r'jobid: (\d+)')


#number of processes
for line in open (path):
    if (r_nprocs.findall(line)):
        nprocs = r_nprocs.findall(line)[0]
        print('\n nprocs = ')
        print(nprocs)
    if (r_jobid.findall(line)):
        jobid = r_jobid.findall(line)[0]
        print('\n jobid = ')
        print(jobid)
    i = i + 1
    if i == 90: 
        break

print('\nheader complete')



print('path: ' +path+"\n")
for chunk in pd.read_csv(path,  sep = '\t', skiprows = 83,comment='#', error_bad_lines=False, header=None,chunksize = 10**9):

    try:
        chunk.columns = ['#<module>', '<rank>', '<record id>', '<counter>', '<value>', 
                         '<file name>', '<mount pt>', '<fs type>']
    except ValueError:

        chunk.columns = ['#<module>', '<rank>', '<record id>', '<counter>', '<value>', 
                         '<file name>']

    #remove negative value records (bad record)
    values = pd.to_numeric(chunk['<value>'],errors = 'coerce').fillna(value = -1)
    df = chunk[values.astype('float64') >= 0]

     #filter for posix entries
    posix = df.loc[df['#<module>'] == 'POSIX']
    #get counts of unique values column wise

    p_uniques = posix.nunique()

    p_uniques

    #build features
    #times
    posix_read_time = df.loc[df['<counter>'] == 'POSIX_F_READ_TIME']['<value>'].astype('float32').agg('sum')
    posix_write_time = df.loc[df['<counter>'] == 'POSIX_F_WRITE_TIME']['<value>'].astype('float32').agg('sum')
    posix_meta_time = df.loc[df['<counter>'] == 'POSIX_F_META_TIME']['<value>'].astype('float32').agg('sum')
    #reads
    posix_bytes_read = df[df['<counter>'] == 'POSIX_BYTES_READ']['<value>'].astype('float32').agg('sum').sum()
    #read_histogram
    posix_bytes_read_100 = df[df['<counter>'] == 'POSIX_SIZE_READ_0_100']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_1K = df[df['<counter>'] == 'POSIX_SIZE_READ_100_1K']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_10K = df[df['<counter>'] == 'POSIX_SIZE_READ_1K_10K']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_100K = df[df['<counter>'] == 'POSIX_SIZE_READ_10K_100K']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_1lM = df[df['<counter>'] == 'POSIX_SIZE_READ_100K_1M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_4M = df[df['<counter>'] == 'POSIX_SIZE_READ_1M_4M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_10M = df[df['<counter>'] == 'POSIX_SIZE_READ_4M_10M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_100M = df[df['<counter>'] == 'POSIX_SIZE_READ_10M_100M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_1G = df[df['<counter>'] == 'POSIX_SIZE_READ_100M_1G']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_read_PLUS = df[df['<counter>'] == 'POSIX_SIZE_READ_PLUS']['<value>'].astype('float32').agg('sum').sum()
    #writes
    posix_bytes_write = df[df['<counter>'] == 'POSIX_BYTES_WRITTEN']['<value>'].astype('float32').agg('sum').sum()
    #write histogram
    posix_bytes_write_100 = df[df['<counter>'] == 'POSIX_SIZE_WRITE_0_100']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_1K = df[df['<counter>'] == 'POSIX_SIZE_WRITE_100_1K']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_10K = df[df['<counter>'] == 'POSIX_SIZE_WRITE_1K_10K']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_100K = df[df['<counter>'] == 'POSIX_SIZE_WRITE_10K_100K']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_1M = df[df['<counter>'] == 'POSIX_SIZE_WRITE_100K_1M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_4M = df[df['<counter>'] == 'POSIX_SIZE_WRITE_1M_4M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_10M = df[df['<counter>'] == 'POSIX_SIZE_WRITE_4M_10M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_100M = df[df['<counter>'] == 'POSIX_SIZE_WRITE_10M_100M']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_1G = df[df['<counter>'] == 'POSIX_SIZE_WRITE_100M_1G']['<value>'].astype('float32').agg('sum').sum()
    posix_bytes_write_PLUS = df[df['<counter>'] == 'POSIX_SIZE_WRITE_PLUS']['<value>'].astype('float32').agg('sum').sum()
    #posix accesses
    posix_opens = df[df['<counter>'] == 'POSIX_OPENS']['<value>'].astype('float32').agg('sum').sum()
    posix_reads = df[df['<counter>'] == 'POSIX_READS']['<value>'].astype('float32').agg('sum').sum()
    posix_writes = df[df['<counter>'] == 'POSIX_WRITES']['<value>'].astype('float32').agg('sum').sum()
    posix_seeks = df[df['<counter>'] == 'POSIX_SEEKS']['<value>'].astype('float32').agg('sum').sum()
    posix_stats = df[df['<counter>'] == 'POSIX_STATS']['<value>'].astype('float32').agg('sum').sum()
    posix_mmaps = df[df['<counter>'] == 'POSIX_MMAPS']['<value>'].astype('float32').agg('sum').sum()
    posix_fsyncs = df[df['<counter>'] == 'POSIX_FSYNCS']['<value>'].astype('float32').agg('sum').sum()
    posix_fdsyncs = df[df['<counter>'] == 'POSIX_FDSYNCS']['<value>'].astype('float32').agg('sum').sum()
    posix_rename_sources = df[df['<counter>'] == 'POSIX_RENAME_SOURCES']['<value>'].astype('float32').agg('sum').sum()
    posix_rename_targets = df[df['<counter>'] == 'POSIX_RENAME_TARGETS']['<value>'].astype('float32').agg('sum').sum()
    posix_renamed_from = df[df['<counter>'] == 'POSIX_RENAMED_FROM']['<value>'].astype('float32').agg('sum').sum()
    posix_renamed_mode = df[df['<counter>'] == 'POSIX_MODE']['<value>'].astype('float32').agg('sum').sum()

    try:
        posix_number_of_files = p_uniques[5]
    except KeyError:
        posix_number_of_files = 0

    #alignment
    posix_f_align = df[df['<counter>'] == 'POSIX_FILE_ALIGNMENT']['<value>'].astype('float32').agg('sum').sum()
    posix_m_align = df[df['<counter>'] == 'POSIX_MEM_ALIGNMENT']['<value>'].astype('float32').agg('sum').sum()

    lustre = df.loc[df['#<module>'] == 'LUSTRE']
    #remove negative value records (bad record)

    lustre = lustre[lustre['<value>'].astype('float64') >= 0]
    lustre

    #get counts of unique values column wise

    l_uniques = lustre.nunique()
    try:
        lustre_number_of_files = l_uniques[5]
    except KeyError:
        lustre_number_of_files = 0

    l_uniques

    lustre_mdts = df[df['<counter>'] == 'LUSTRE_MDTS']['<value>'].astype('float32').agg('mean')

    #number of servers for file system
    lustre_osts = df[df['<counter>'] == 'LUSTRE_OSTS']['<value>'].astype('float32').agg('mean')
    lustre_mdts = df[df['<counter>'] == 'LUSTRE_MDTS']['<value>'].astype('float32').agg('mean')
    #lustre striping
    lustre_stripe_size = df[df['<counter>'] == 'LUSTRE_STRIPE_SIZE']['<value>'].astype('float32').agg('mean')
    lustre_stripe_offset = df[df['<counter>'] == 'LUSTRE_STRIPE_OFFSET']['<value>'].astype('float32').agg('sum')
    lustre_stripe_width = df[df['<counter>'] == 'LUSTRE_STRIPE_WIDTH']['<value>'].astype('float32').agg('sum')

    lustre_number_of_osts = df[df['<counter>'] == 'LUSTRE_OST_ID_0'].nunique()['<value>']



    ###############################################

    out = pd.DataFrame()

    out['posix_read_time'] = [posix_read_time]
    out['posix_write_time'] = [posix_write_time]
    out['posix_meta_time'] = [posix_meta_time]
    #reads
    out['posix_bytes_read'] = [posix_bytes_read]
    #read_histogram
    out['posix_bytes_read_100'] = [posix_bytes_read_100]
    out['posix_bytes_read_1K'] = [posix_bytes_read_1K]
    out['posix_bytes_read_10K'] = [posix_bytes_read_10K]
    out['posix_bytes_read_100K'] = [posix_bytes_read_100K]
    out['posix_bytes_read_1lM'] = [posix_bytes_read_1lM]
    out['posix_bytes_read_4M'] = [posix_bytes_read_4M]
    out['posix_bytes_read_10M'] = [posix_bytes_read_10M]
    out['posix_bytes_read_100M'] = [posix_bytes_read_100M]
    out['posix_bytes_read_1G'] = [posix_bytes_read_1G]
    out['posix_bytes_read_PLUS'] = [posix_bytes_read_PLUS]
    #writes
    out['posix_bytes_write'] = [posix_bytes_write]
    #write histogram
    out['posix_bytes_write_100'] = [posix_bytes_write_100]
    out['posix_bytes_write_1K'] = [posix_bytes_write_1K]
    out['posix_bytes_write_10K'] = [posix_bytes_write_10K]
    out['posix_bytes_write_100K'] = [posix_bytes_write_100K]
    out['posix_bytes_write_1M'] = [posix_bytes_write_1M]
    out['posix_bytes_write_4M'] = [posix_bytes_write_4M]
    out['posix_bytes_write_10M'] = [posix_bytes_write_10M]
    out['posix_bytes_write_100M'] = [posix_bytes_write_100M]
    out['posix_bytes_write_1G'] = [posix_bytes_write_1G]
    out['posix_bytes_write_PLUS'] = [posix_bytes_write_PLUS]
    #posix accesses
    out['posix_opens'] = [posix_opens]
    out['posix_reads'] = [posix_reads]
    out['posix_writes'] = [posix_writes]
    out['posix_seeks'] = [posix_seeks]
    out['posix_stats'] = [posix_stats]
    out['posix_mmaps'] = [posix_mmaps]
    out['posix_fsyncs'] = [posix_fsyncs]
    out['posix_fdsyncs'] = [posix_fdsyncs]
    out['posix_rename_sources'] = [posix_rename_sources]
    out['posix_rename_targets'] = [posix_rename_targets]
    out['posix_renamed_from'] = [posix_renamed_from]
    out['posix_renamed_mode'] = [posix_renamed_mode]

    out['posix_number_of_files'] = [posix_number_of_files]

    #number of processes
    out['nprocs'] = [nprocs]

    #alignment
    out['posix_f_align'] = [posix_f_align]
    out['posix_m_align'] = [posix_m_align]

    out['lustre_number_of_files'] = [lustre_number_of_files]
    out['lustre_mdts'] = [lustre_mdts]
    #number of servers for file system
    out['lustre_osts'] = [lustre_osts]
    out['lustre_mdts'] = [lustre_mdts]
    #lustre striping
    out['lustre_stripe_size'] = [lustre_stripe_size]
    out['lustre_stripe_offset'] = [lustre_stripe_offset]
    out['lustre_stripe_width'] = [lustre_stripe_width]
    out['lustre_number_of_osts'] = [lustre_number_of_osts]
    out['jobid'] = [jobid]

    features_scan = features_scan.append(out)

    del out
    del df
    del posix
    del chunk

"""     i = i + 1
    if i%200 == 0:    
        print('chunks written: ' + str(i) + '\n')
        features_scan.to_csv(out_path + ".csv") 
        del features_scan
        features_scan = pd.DataFrame() """
#features_scan = features_scan.sum()

features_scan.to_csv(out_path )

print('complete')
