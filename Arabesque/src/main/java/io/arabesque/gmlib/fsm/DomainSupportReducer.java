package io.arabesque.gmlib.fsm;

import io.arabesque.aggregation.reductions.ReductionFunction;

public class DomainSupportReducer
   extends ReductionFunction<DomainSupport> {
    @Override
    public DomainSupport reduce(DomainSupport k1, DomainSupport k2) {
        k1.aggregate(k2);

        return k1;
    }
}
