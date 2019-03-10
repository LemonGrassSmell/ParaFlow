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

public class FilterBlockIndexByFiberSortColAction extends Action
{
    @Override
    public ActionResponse act(ActionResponse input, Connection connection) throws ParaFlowException
    {
        Optional<Object> paramOp = input.getParam();
        Optional<Object> tblIdOp = input.getProperties("tblId");
        if (paramOp.isPresent() && tblIdOp.isPresent()) {
            long tblId = (long) tblIdOp.get();
            MetaProto.FilterBlockIndexByFiberSortColParam filterBlockIndexByFiberSortColParam
                    = (MetaProto.FilterBlockIndexByFiberSortColParam) paramOp.get();
            long fiberValue = filterBlockIndexByFiberSortColParam.getValue().getValue();
            int sortColumnId = filterBlockIndexByFiberSortColParam.getSortColumnId();
            ArrayList<String> result = new ArrayList<>();
            String sqlStatement;
            if (filterBlockIndexByFiberSortColParam.getTimeBegin() == -1
                    && filterBlockIndexByFiberSortColParam.getTimeEnd() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiberSortCol(tblId, fiberValue, sortColumnId);
            }
            else if (filterBlockIndexByFiberSortColParam.getTimeBegin() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiberSortColEnd(
                        tblId,
                        fiberValue,
                        sortColumnId,
                        filterBlockIndexByFiberSortColParam.getTimeEnd());
            }
            else if (filterBlockIndexByFiberSortColParam.getTimeEnd() == -1) {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiberSortColBegin(
                        tblId,
                        fiberValue,
                        sortColumnId,
                        filterBlockIndexByFiberSortColParam.getTimeBegin());
            }
            else {
                //query
                sqlStatement = SQLTemplate.filterBlockIndexByFiberSortColBeginEnd(
                        tblId,
                        fiberValue,
                        sortColumnId,
                        filterBlockIndexByFiberSortColParam.getTimeBegin(),
                        filterBlockIndexByFiberSortColParam.getTimeEnd());
            }
            ResultList resultList = connection.executeQuery(sqlStatement);
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
