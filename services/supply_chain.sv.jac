import from byllm.lib { Model }

glob llm: Model = Model(model_name="gpt-4o-mini");

node Package {
    has name: str;
    has weekly_downloads: int = 0;
    has maintainer_count: int = 1;
    has compromised: bool = False;
    has compromise_depth: int = -1;
    has threat_summary: str = "";
}

edge DependsOn {
    has version_constraint: str = "*";
    has is_direct: bool = True;
}

def assess_package_threat(
    package_name: str,
    weekly_downloads: int,
    compromise_depth: int,
    dependent_count: int
) -> str by llm();

def get_or_create_package(name: str, weekly_downloads: int = 0, maintainer_count: int = 1) -> Package {
    for pkg in [root()-->][?:Package] {
        if pkg.name == name {
            if weekly_downloads > 0 { pkg.weekly_downloads = weekly_downloads; }
            if maintainer_count > 0 { pkg.maintainer_count = maintainer_count; }
            return pkg;
        }
    }
    return (root() ++> Package(
        name=name,
        weekly_downloads=weekly_downloads,
        maintainer_count=maintainer_count
    ))[0];
}

def package_to_dict(pkg: Package) -> dict {
    dependencies = [{
        "name": dep.name,
        "version_constraint": "*",
        "is_direct": True
    } for dep in [pkg-->][?:Package]];

    dependent_count = len([pkg<--][?:Package]);

    return {
        "id": jid(pkg),
        "name": pkg.name,
        "weekly_downloads": pkg.weekly_downloads,
        "maintainer_count": pkg.maintainer_count,
        "compromised": pkg.compromised,
        "compromise_depth": pkg.compromise_depth,
        "threat_summary": pkg.threat_summary,
        "dependent_count": dependent_count,
        "dependencies": dependencies
    };
}

def:pub build_graph(packages: list[dict]) -> dict {
    existing = [root()-->][?:Package];
    for pkg in existing {
        del pkg;
    }

    for item in packages {
        get_or_create_package(
            name=item["name"],
            weekly_downloads=item.get("weekly_downloads", 0),
            maintainer_count=item.get("maintainer_count", 1)
        );
    }

    for item in packages {
        pkg = get_or_create_package(
            name=item["name"],
            weekly_downloads=item.get("weekly_downloads", 0),
            maintainer_count=item.get("maintainer_count", 1)
        );

        deps: list = item.get("dependencies", []);
        for dep_info in deps {
            dep_name = "";
            version_constraint = "*";
            is_direct = True;

            if isinstance(dep_info, str) {
                dep_name = dep_info;
            } else {
                dep_name = dep_info["name"];
                version_constraint = dep_info.get("version_constraint", "*");
                is_direct = dep_info.get("is_direct", True);
            }

            dep_pkg = get_or_create_package(name=dep_name);

            already_linked = False;
            for existing_dep in [pkg-->][?:Package] {
                if existing_dep.name == dep_pkg.name {
                    already_linked = True;
                }
            }

            if not already_linked {
                pkg +>:DependsOn(
                    version_constraint=version_constraint,
                    is_direct=is_direct
                ):+> dep_pkg;
            }
        }
    }

    return {
        "success": True,
        "package_count": len([root()-->][?:Package])
    };
}

def:pub reset_graph() -> dict {
    w = Reset();
    root() spawn w;
    return {"success": True, "reset_count": w.reset_count};
}

def:pub get_graph() -> dict {
    w = GetGraph();
    root() spawn w;
    return {
        "success": True,
        "package_count": len(w.packages),
        "packages": w.packages
    };
}

def:pub infect_package(package_name: str) -> dict {
    w = Infect(target_name=package_name);
    root() spawn w;
    if w.compromised_ids {
        return {
            "success": True,
            "target": package_name,
            "compromised_count": len(w.compromised_ids),
            "affected_weekly_downloads": w.total_damage,
            "compromised_package_ids": w.compromised_ids,
            "compromised_packages": w.compromised_packages
        };
    }
    return {"success": False, "error": "Package not found", "target": package_name};
}

def:pub assess_compromise() -> dict {
    w = Assess();
    root() spawn w;
    return {"success": True, "assessed": w.assessed};
}

walker :pub BuildGraph {
    has packages: list[dict] = [];

    can build with Root entry {
        report build_graph(self.packages);
    }
}

walker :pub Infect {
    has target_name: str = "";
    has compromised_ids: list[str] = [];
    has compromised_packages: list[dict] = [];
    has total_damage: int = 0;
    has found: bool = False;

    can start with Root entry {
        for pkg in [-->][?:Package] {
            if pkg.name == self.target_name {
                self.found = True;
                visit pkg;
                return;
            }
        }
    }

    can spread with Package entry {
        if jid(here) in self.compromised_ids {
            return;
        }

        if here.compromise_depth < 0 {
            if len(self.compromised_ids) == 0 {
                here.compromise_depth = 0;
            } else {
                parent_depths = [parent.compromise_depth for parent in [-->][?:Package] if parent.compromise_depth >= 0];
                here.compromise_depth = min(parent_depths) + 1 if parent_depths else 1;
            }
        }

        here.compromised = True;
        self.compromised_ids.append(jid(here));
        self.compromised_packages.append({
            "name": here.name,
            "depth": here.compromise_depth,
            "weekly_downloads": here.weekly_downloads
        });
        self.total_damage += here.weekly_downloads;

        visit [<--][?:Package];
    }

    can finish with Root exit {
        if self.found {
            report {
                "success": True,
                "target": self.target_name,
                "compromised_count": len(self.compromised_ids),
                "affected_weekly_downloads": self.total_damage,
                "compromised_packages": self.compromised_packages
            };
        } else {
            report {"success": False, "error": "Package not found", "target": self.target_name};
        }
    }
}

walker :pub Assess {
    has assessed: int = 0;

    can start with Root entry {
        visit [-->][?:Package];
        report {"success": True, "assessed": self.assessed};
    }

    can summarize with Package entry {
        if here.compromised {
            dependent_count = len([<--][?:Package]);
            here.threat_summary = assess_package_threat(
                package_name=here.name,
                weekly_downloads=here.weekly_downloads,
                compromise_depth=here.compromise_depth,
                dependent_count=dependent_count
            );
            self.assessed += 1;
        }
    }
}

walker :pub Reset {
    has reset_count: int = 0;

    can start with Root entry {
        visit [-->][?:Package];
        report {"success": True, "reset_count": self.reset_count};
    }

    can clear with Package entry {
        here.compromised = False;
        here.compromise_depth = -1;
        here.threat_summary = "";
        self.reset_count += 1;
    }
}

walker :pub GetGraph {
    has packages: list[dict] = [];

    can start with Root entry {
        visit [-->][?:Package];
        report {
            "success": True,
            "package_count": len(self.packages),
            "packages": self.packages
        };
    }

    can collect with Package entry {
        self.packages.append(package_to_dict(here));
    }
}
