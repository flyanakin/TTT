import os
import pandas as pd
from dagster import (
    IOManager,
    io_manager,
    OutputContext,
    InputContext,
)
from TreasureTrove.resources import WAREHOUSE_PATH


class CsvIOManager(IOManager):
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        root_path = WAREHOUSE_PATH
        path = context.asset_key.path
        dir_path = None
        for dir in path[:-1]:
            if dir_path is None:
                dir_path = os.path.join(root_path, dir)
            else:
                dir_path = os.path.join(dir_path, dir)
        file_name = context.asset_key.path[-1]

        # 检查是否存在分区键
        partition_key = context.partition_key if context.has_partition_key else None
        context.log.debug(f"分区键:{partition_key}")
        if partition_key:
            if "|" in partition_key:
                key_dict = context.partition_key.keys_by_dimension
                keys = list(key_dict.keys())
                file_name_with_partition = file_name
                for key in reversed(keys):
                    file_name_with_partition = file_name_with_partition + '_' + key_dict[key]
            else:
                file_name_with_partition = f"{file_name}_{partition_key}"
        else:
            file_name_with_partition = file_name

        if not os.path.exists(dir_path):
            context.log.info(f"文件夹{dir_path}不存在，创建文件夹")
            os.makedirs(dir_path)

        file_path = os.path.join(dir_path, f"{file_name_with_partition}.csv")
        context.log.info(f"输出文件:{file_name_with_partition}.csv\n到文件夹{dir_path}")
        return obj.to_csv(file_path, index=False)

    def load_input(self, context: InputContext):
        root_path = WAREHOUSE_PATH
        path = context.upstream_output.asset_key.path
        dir_path = None
        for dir in path[:-1]:
            if dir_path is None:
                dir_path = os.path.join(root_path, dir)
            else:
                dir_path = os.path.join(dir_path, dir)

        if not os.path.exists(dir_path):
            raise Exception(f"文件夹{dir_path}不存在")

        file_name = context.upstream_output.asset_key.path[-1]

        # 检查是否存在分区键
        partition_key = context.partition_key if context.has_partition_key else None
        if partition_key:
            if "|" in partition_key:
                key_dict = context.partition_key.keys_by_dimension
                keys = list(key_dict.keys())
                context.log.debug(f"key: {keys}")
                file_name_with_partition = file_name
                for key in reversed(keys):
                    file_name_with_partition = file_name_with_partition + '_' + key_dict[key]
            else:
                file_name_with_partition = f"{file_name}_{partition_key}"
        else:
            file_name_with_partition = file_name

        file_path = os.path.join(dir_path, f"{file_name_with_partition}.csv")
        context.log.info(f"读取文件:{file_name_with_partition}.csv \n从文件夹{dir_path}")

        return pd.read_csv(file_path)


@io_manager
def csv_io_manager():
    return CsvIOManager()
