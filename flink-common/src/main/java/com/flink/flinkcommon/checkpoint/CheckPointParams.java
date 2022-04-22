package com.flink.flinkcommon.checkpoint;

import com.flink.flinkcommon.constant.SystemConstant;
import com.flink.flinkcommon.enums.CheckPointParameterEnums;
import com.flink.flinkcommon.enums.StateBackendEnum;
import com.flink.flinkcommon.model.CheckPointParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;


@Slf4j
public class CheckPointParams {

    /**
     * 构建checkPoint参数
     *
     */
    public static CheckPointParam buildCheckPointParam(ParameterTool parameterTool){

        String checkpointDir = parameterTool.get(CheckPointParameterEnums.checkpointDir.name(), SystemConstant.SPACE);
        //如果checkpointDir为空不启用CheckPoint
        if (StringUtils.isEmpty(checkpointDir)) {
            return null;
        }

        String checkpointingMode = parameterTool.get(CheckPointParameterEnums.checkpointingMode.name(),
                CheckpointingMode.EXACTLY_ONCE.name());

        String checkpointInterval = parameterTool.get(CheckPointParameterEnums.checkpointInterval.name());

        String checkpointTimeout = parameterTool.get(CheckPointParameterEnums.checkpointTimeout.name(), String.valueOf(SystemConstant.DEFAULT_CHECKPOINT_TIMEOUT));

        String tolerableCheckpointFailureNumber =
                parameterTool.get(CheckPointParameterEnums.tolerableCheckpointFailureNumber.name(), SystemConstant.SPACE);

        String asynchronousSnapshots = parameterTool.get(CheckPointParameterEnums.asynchronousSnapshots.name(), SystemConstant.SPACE);

        String externalizedCheckpointCleanup =
                parameterTool.get(CheckPointParameterEnums.externalizedCheckpointCleanup.name(), SystemConstant.SPACE);

        String stateBackendType = parameterTool.get(CheckPointParameterEnums.stateBackendType.name(), StateBackendEnum.MEMORY.getType());

        String enableIncremental = parameterTool.get(CheckPointParameterEnums.enableIncremental.name(), SystemConstant.SPACE);

        CheckPointParam checkPointParam = new CheckPointParam();

        // TODO
        if(StringUtils.isNotEmpty(checkpointDir)){
            checkPointParam.setCheckpointDir(checkpointDir);
        }

        if (StringUtils.isNotEmpty(asynchronousSnapshots)) {
            checkPointParam.setAsynchronousSnapshots(Boolean.parseBoolean(asynchronousSnapshots));
        }

        checkPointParam.setStateBackendEnum(StateBackendEnum.getStateBackend(stateBackendType));

        checkPointParam.setCheckpointingMode(checkpointingMode);

        if(StringUtils.isNotEmpty(checkpointInterval)){
            checkPointParam.setCheckpointInterval(Long.valueOf(checkpointInterval));
        }

        checkPointParam.setCheckpointTimeout(Long.valueOf(checkpointTimeout));

        if (StringUtils.isNotEmpty(tolerableCheckpointFailureNumber)) {
            checkPointParam.setTolerableCheckpointFailureNumber(Integer.valueOf(tolerableCheckpointFailureNumber));
        }

        if (StringUtils.isNotEmpty(externalizedCheckpointCleanup)) {
            checkPointParam.setExternalizedCheckpointCleanup(externalizedCheckpointCleanup);
        }

        if (StringUtils.isNotEmpty(enableIncremental)) {
            checkPointParam.setEnableIncremental(Boolean.parseBoolean(enableIncremental.trim()));
        }
        log.info("checkPointParam={}", checkPointParam);
        return checkPointParam;

    }

}
