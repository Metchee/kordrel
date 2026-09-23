use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    #[serde(default)]
    pub max_parallel: Option<i64>,
    pub tasks: Vec<Task>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub timeout_seconds: Option<i64>,
    #[serde(default)]
    pub retries: Option<i64>,
    #[serde(default)]
    pub max_attempts: Option<i64>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

impl Task {
    pub fn attempts(&self) -> i64 {
        self.max_attempts
            .unwrap_or_else(|| self.retries.unwrap_or(0) + 1)
    }
}

impl Workflow {
    pub fn load(path: &Path) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("lecture de {}", path.display()))?;
        let workflow: Self = serde_yaml::from_str(&content)
            .with_context(|| format!("YAML invalide dans {}", path.display()))?;
        workflow.validate()?;
        Ok(workflow)
    }

    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        if self.name.trim().is_empty() {
            errors.push("nom du workflow vide".to_string());
        }
        if self.tasks.is_empty() {
            errors.push("le workflow ne contient aucune tâche".to_string());
        }
        if self.max_parallel.is_some_and(|n| !(1..=1024).contains(&n)) {
            errors.push("max_parallel doit être entre 1 et 1024".to_string());
        }
        let mut ids = HashSet::new();
        for task in &self.tasks {
            if task.id.trim().is_empty() {
                errors.push("identifiant de tâche vide".to_string());
            }
            if !ids.insert(task.id.as_str()) {
                errors.push(format!("tâche '{}' : identifiant dupliqué", task.id));
            }
            if task.command.trim().is_empty() {
                errors.push(format!("tâche '{}' : commande vide", task.id));
            }
            if task
                .timeout_seconds
                .is_some_and(|n| !(1..=604_800).contains(&n))
            {
                errors.push(format!(
                    "tâche '{}' : timeout_seconds doit être entre 1 et 604800",
                    task.id
                ));
            }
            if task.retries.is_some_and(|n| !(0..=1000).contains(&n)) {
                errors.push(format!(
                    "tâche '{}' : retries doit être entre 0 et 1000",
                    task.id
                ));
            }
            if task.max_attempts.is_some_and(|n| !(1..=1001).contains(&n)) {
                errors.push(format!(
                    "tâche '{}' : max_attempts doit être entre 1 et 1001",
                    task.id
                ));
            }
            if task.retries.is_some() && task.max_attempts.is_some() {
                errors.push(format!(
                    "tâche '{}' : choisir retries ou max_attempts",
                    task.id
                ));
            }
            let mut deps = HashSet::new();
            for dep in &task.dependencies {
                if !deps.insert(dep) {
                    errors.push(format!(
                        "tâche '{}' : dépendance '{}' dupliquée",
                        task.id, dep
                    ));
                }
            }
        }
        for task in &self.tasks {
            for dep in &task.dependencies {
                if !ids.contains(dep.as_str()) {
                    errors.push(format!(
                        "tâche '{}' : dépendance inconnue '{}'",
                        task.id, dep
                    ));
                }
            }
        }
        if errors.is_empty() {
            let by_id: HashMap<&str, &Task> =
                self.tasks.iter().map(|t| (t.id.as_str(), t)).collect();
            let mut visited = HashSet::new();
            let mut active = HashSet::new();
            fn visit<'a>(
                id: &'a str,
                map: &HashMap<&'a str, &'a Task>,
                visited: &mut HashSet<&'a str>,
                active: &mut HashSet<&'a str>,
                errors: &mut Vec<String>,
            ) {
                if active.contains(id) {
                    errors.push(format!("cycle impliquant la tâche '{id}'"));
                    return;
                }
                if visited.contains(id) {
                    return;
                }
                active.insert(id);
                for dep in &map[id].dependencies {
                    visit(dep, map, visited, active, errors);
                }
                active.remove(id);
                visited.insert(id);
            }
            for task in &self.tasks {
                visit(&task.id, &by_id, &mut visited, &mut active, &mut errors);
            }
        }
        if !errors.is_empty() {
            bail!(errors.join("\n"));
        }
        Ok(())
    }

    pub fn groups(&self, parallel: usize) -> Vec<Vec<String>> {
        let mut done = HashSet::new();
        let mut groups = Vec::new();
        while done.len() < self.tasks.len() {
            let ready: Vec<_> = self
                .tasks
                .iter()
                .filter(|t| {
                    !done.contains(&t.id) && t.dependencies.iter().all(|d| done.contains(d))
                })
                .take(parallel)
                .map(|t| t.id.clone())
                .collect();
            if ready.is_empty() {
                break;
            }
            done.extend(ready.iter().cloned());
            groups.push(ready);
        }
        groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn task(id: &str, deps: &[&str]) -> Task {
        Task {
            id: id.into(),
            command: "true".into(),
            args: vec![],
            dependencies: deps.iter().map(|s| s.to_string()).collect(),
            workdir: None,
            timeout_seconds: None,
            retries: None,
            max_attempts: None,
            env: HashMap::new(),
        }
    }
    #[test]
    fn cycle_and_unknown_dependency() {
        let w = Workflow {
            name: "x".into(),
            max_parallel: None,
            tasks: vec![task("a", &["b"]), task("b", &["a"])],
        };
        assert!(w.validate().unwrap_err().to_string().contains("cycle"));
        let w = Workflow {
            name: "x".into(),
            max_parallel: None,
            tasks: vec![task("a", &["missing"])],
        };
        assert!(w
            .validate()
            .unwrap_err()
            .to_string()
            .contains("dépendance inconnue 'missing'"));
    }

    #[test]
    fn duplicate_and_invalid_limits_name_tasks() {
        let mut a = task("a", &[]);
        a.timeout_seconds = Some(0);
        a.retries = Some(-1);
        let w = Workflow {
            name: "x".into(),
            max_parallel: Some(0),
            tasks: vec![a, task("a", &[])],
        };
        let message = w.validate().unwrap_err().to_string();
        assert!(message.contains("identifiant dupliqué"));
        assert!(message.contains("tâche 'a' : timeout_seconds"));
        assert!(message.contains("tâche 'a' : retries"));
        assert!(message.contains("max_parallel"));
    }
}
