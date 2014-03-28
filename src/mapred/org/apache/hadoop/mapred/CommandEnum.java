package org.apache.hadoop.mapred;

import java.util.Map;
import java.util.HashMap;

interface IntegerEnum {
    int getNum();
}
class EnumEnhancer<E extends Enum<E> & IntegerEnum> {
    private Map<Integer, E> lookup;

    public EnumEnhancer(E... values) {
        lookup = new HashMap<Integer, E>();
        for(E e : values)
            lookup.put(e.getNum(), e);
    }
    public E lookup(int commandNum) {
        return lookup.get(commandNum);
    }
}
enum HadoopToControllerCommand implements IntegerEnum {
    ERROR(-1), EOF(0), EXIT(1), MR_JOB_CONTENT(2);

    private static final EnumEnhancer<HadoopToControllerCommand> enhancer =
        new EnumEnhancer<HadoopToControllerCommand>(values());

    private int command;
    HadoopToControllerCommand(int command) {
        this.command = command;
    }
    @Override
    public int getNum() {
        return command;
    }
    public static HadoopToControllerCommand lookup(int commandNum) {
        return enhancer.lookup(commandNum);
    }
}
enum ControllerToHadoopCommand implements IntegerEnum {
    ERROR(-1), EOF(0), EXIT(1), TOPO_CONTENT(2);

    private static final EnumEnhancer<ControllerToHadoopCommand> enhancer =
        new EnumEnhancer<ControllerToHadoopCommand>(values());

    private int command;
    ControllerToHadoopCommand(int command) {
        this.command = command;
    }
    @Override
    public int getNum() {
        return command;
    }
    public static ControllerToHadoopCommand lookup(int commandNum) {
        return enhancer.lookup(commandNum);
    }
}
enum MRPhase implements IntegerEnum {
    ERROR(-1), STARTING(0), MAP(1), SHUFFLE(2), SORT(3), REDUCE(4), CLEANUP(5);

    private static final EnumEnhancer<MRPhase> enhancer =
        new EnumEnhancer<MRPhase>(values());

    private int phase;
    MRPhase(int phase) {
        this.phase = phase;
    }
    @Override
    public int getNum() {
        return phase;
    }
    public static MRPhase lookup(int phaseNum) {
        return enhancer.lookup(phaseNum);
    }
}