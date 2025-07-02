class AzureBlobCrossEnvironmentDataTransfer:
    def __init__(self):

        self.current_env=   f"{mssparkutils.env.getWorkspaceName()[8:15]}"
        self.dev_storage=   "adlsbauapp63462"
        self.prod_storage=  "adlsbauapp63442"
        self.bronze_path=   f"abfss://bronze@{{storage}}.dfs.core.windows.net/"
        self.silver_path=   f"abfss://silver@{{storage}}.dfs.core.windows.net/"
        self.gold_path=     f"abfss://gold@{{storage}}.dfs.core.windows.net/"
    
    def _build_path(self, source, sink, container, relative_path):
        "Create path"
        if container not in ["bronze", "silver", "gold"]:
            raise ValueError("Container must be one of: 'bronze', 'silver', or 'gold'")

        # Pick correct storage account
        source_storage = self.dev_storage if source.lower() == "dev" else self.prod_storage
        sink_storage = self.dev_storage if sink.lower() == "dev" else self.prod_storage

        # Build base path
        base_path = {
            "bronze": self.bronze_path,
            "silver": self.silver_path,
            "gold": self.gold_path
        }[container]

        # path
        source_path, sink_path= base_path.format(storage=source_storage), base_path.format(storage=sink_storage)

        # Build full path
        full_source_path, full_sink_path = f"{source_path}{relative_path.strip('/')}/",  f"{sink_path}{relative_path.strip('/')}/"

        return full_source_path, full_sink_path

    def _check_data_file_path(self, source, sink, container, relative_path, return_path=False):
        " This func checks the number of files in a folder and returns the directory tree"
        try:
            # develop path
            full_source_path, full_sink_path= self._build_path(source, sink, container, relative_path)
            # check directory
            directory = mssparkutils.fs.ls(full_path)
            print(f"[{container.upper()}] Directory Listing for: {full_path}")
            for file in directory:
                print(file.name)
            return directory if return_path else None
        except Exception as e:
            print(f"Error accessing path: {full_path} -> {e}")
            return [] if return_path else None
    
    def _blob_storage_data_transfer(self, source, sink, container, relative_path):
        " This func takes args (source, sink) and then syncs data between them"
        try:
            # develop path
            full_source_path, full_sink_path= self._build_path(source, sink, container, relative_path)
            try:
                # copy file from source to sink
                mssparkutils.fs.cp(full_source_path, full_sink_path, recurse=True)
                print("files copied successfully")
            except Exception as e:
                print(f"Error copying files: {full_path} -> {e}")

# Test connector
# if __name__=="__main__":
    # transfer = AzureBlobCrossEnvironmentDataTransfer()

    # # Check files in bronze container in Dev environment
    # transfer._check_data_file_path(source="dev", sink="prod", container="bronze", relative_path="api/dp360")

    # # Copy files from dev to prod
    # transfer._blob_storage_data_transfer(source="dev", sink="prod", container="bronze", relative_path="api/dp360")