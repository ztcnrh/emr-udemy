def to_files(df, tgt_dir, file_format):
    df.write. \
        partitionBy('year', 'month', 'dayofmonth'). \
        mode('append'). \
        format(file_format). \
        save(tgt_dir)
