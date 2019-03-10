package cn.edu.ruc.iir.paraflow.metaserver.action;

import cn.edu.ruc.iir.paraflow.commons.exceptions.ActionParamNotValidException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.FilterBlockIndexNotFoundException;
import cn.edu.ruc.iir.paraflow.commons.exceptions.ParaFlowException;
import cn.edu.ruc.iir.paraflow.metaserver.connection.Connection;
import cn.edu.ruc.iir.paraflow.metaserver.connection.ResultList;
import cn.edu.ruc.iir.paraflow.metaserver.proto.MetaProto;
import cn.edu.ruc.iir.paraflow.metaserver.utils.SQLTemplate;

import java.util.ArrayList;
import java.util.Optional;

public class FilterBlockIndexBySortColAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> tblIdOp = input.getProperties("tblId");
        if (paramOp.isPresent() && tblIdOp.isPresent()) {
            long tblId = (long) tblIdOp.get();
            MetaProto.FilterBlockIndexBySortColParam filterBlockIndexBySortColParam
                    = (MetaProto.FilterBlockIndexBySortColParam) paramOp.get();
            ArrayList<String> result = new ArrayList<>();
            String sqlStatement;
            if (filterBlockIndexBySortColParam.getTimeBegin() == -1
                    && filterBlockIndexBySortColParam.getTimeEnd() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexBySortCol(
                        tblId,
                        filterBlockIndexBySortColParam.getSortColumnId());
            }
            else if (filterBlockIndexBySortColParam.getTimeBegin() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexBySortColEnd(
                        tblId,
                        filterBlockIndexBySortColParam.getSortColumnId(),
                        filterBlockIndexBySortColParam.getTimeEnd());
            }
            else if (filterBlockIndexBySortColParam.getTimeEnd() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexBySortColBegin(
                        tblId,
                        filterBlockIndexBySortColParam.getSortColumnId(),
                        filterBlockIndexBySortColParam.getTimeBegin());
            }
            else {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexBySortColBeginEnd(
                        tblId,
                        filterBlockIndexBySortColParam.getSortColumnId(),
                        filterBlockIndexBySortColParam.getTimeBegin(),
                        filterBlockIndexBySortColParam.getTimeEnd());
            }
            ResultList resultList = connection.executeQuery(sqlStatement);
            System.out.println(sqlStatement);
            MetaProto.StringListType stringList;
            if (!resultList.isEmpty()) {
                int size = resultList.size();
                for (int i = 0; i < size; i++) {
                    result.add(resultList.get(i).get(0));
                }
                //result
                stringList = MetaProto.StringListType.newBuilder()
                        .addAllStr(result)
                        .setIsEmpty(false)
                        .build();
                input.setParam(stringList);
            }
            else {
                throw new FilterBlockIndexNotFoundException();
            }
        }
        else {
            throw new ActionParamNotValidException();
        }
        return input;
    }
}
