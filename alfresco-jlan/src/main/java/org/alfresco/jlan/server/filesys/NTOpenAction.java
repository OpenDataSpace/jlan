package org.alfresco.jlan.server.filesys;

public enum NTOpenAction {
    SUPERSEDE(0, FileAction.TruncateExisting + FileAction.CreateNotExist, "Supersede"), // supersede if exists, else create a new file
    OPEN(1, FileAction.OpenIfExists, "Open"), // only open if the file exists
    CREATE(2, FileAction.CreateNotExist, "Create"), // create if file does not exist, else fail
    OPEN_IF(3, FileAction.OpenIfExists + FileAction.CreateNotExist, "OpenIf"), // open if exists else create
    OVERWRITE(4, FileAction.TruncateExisting, "Overwrite"), // overwrite if exists, else fail
    OVERWRITE_IF(5, FileAction.TruncateExisting + FileAction.CreateNotExist, "OverwriteIf"); // overwrite if exists, else create

    private final int lmValue;
    private final int value;
    private final String name;

    private NTOpenAction(final int value, final int lmValue, final String name) {
        this.lmValue = lmValue;
        this.value = value;
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public int getValue() {
        return value;
    }

    /**
     * Convert a Core/LanMan open action to an NT open action
     *
     * @param openAction
     *            int
     * @return int
     */
    public static NTOpenAction convertToNTOpenAction(int openAction) {
        // Convert the Core/LanMan SMB dialect open action to an NT open action
        NTOpenAction converted = NTOpenAction.OPEN;
        for (NTOpenAction action : NTOpenAction.values()) {
            if (action.lmValue == openAction)
                converted = action;
        }
        return converted;
    }

    public static NTOpenAction fromValue(int openAction) {
        for (NTOpenAction action : NTOpenAction.values()) {
            if (action.value == openAction) {
                return action;
            }
        }
        return null;
    }
}
