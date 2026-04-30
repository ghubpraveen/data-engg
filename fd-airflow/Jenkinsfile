@Library('cicd-library') _

pipeline {
    agent any

    triggers {
        cron('H/5 * * * *')  // polling (temporary)
    }

    options {
        disableConcurrentBuilds(abortPrevious: false) // ✅ allow parallel runs
        buildDiscarder(logRotator(numToKeepStr: '5'))
        timestamps()
    }

    environment {
        CONTROL_REPO = "https://github.com/praveen-devops-labs/env-control-repo.git"
        STATE_DIR    = "/u01/jenkins/jenkins/build-state"
    }

    stages {

        // 🔷 Start
        stage('Start') {
            steps {
                script {
                    gchatNotify("🚀 Orchestrator started")
                }
            }
        }

        // 🔷 Load Mapping
        stage('Load Mapping') {
            steps {
                script {

                    dir('env-control') {
                        deleteDir()

                        git url: env.CONTROL_REPO,
                            branch: 'main',
                            credentialsId: 'github-token'
                    }

                    def config = readYaml file: 'env-control/mappings/current.yaml'

                    if (!config?.mappings) {
                        error "❌ mappings missing in YAML"
                    }

                    env.MAP = groovy.json.JsonOutput.toJson(config.mappings)

                    echo "📄 Mapping loaded"
                }
            }
        }

        // 🔷 Process (Polling + Trigger)
        stage('Process') {
            steps {
                script {

                    def parsed = readJSON text: env.MAP
                    def jobs = [:]
                    def changesDetected = false

                    parsed.each { key, value ->

                        def app     = value.app.toString()
                        def repo    = value.repo.toString()
                        def branch  = value.branch.toString()
                        def envName = value.env.toString()

                        // 🔥 Only DEV
                        if (envName != 'dev') {
                            return
                        }

                        def commit = ""

                        // ✅ SAFE credential usage (no interpolation warning)
                        withCredentials([usernamePassword(
                            credentialsId: 'github-token',
                            usernameVariable: 'GIT_USER',
                            passwordVariable: 'GIT_PASS'
                        )]) {

                            commit = sh(
                                script: '''
                                git ls-remote https://$GIT_USER:$GIT_PASS@''' + repo.replace('https://','') + ''' refs/heads/''' + branch + ''' | cut -f1
                                ''',
                                returnStdout: true
                            ).trim()
                        }

                        if (!commit) {
                            echo "⚠️ Cannot fetch commit for ${app}"
                            return
                        }

                        def stateFile = "${env.STATE_DIR}/${app}/dev.commit"

                        def lastBuilt = sh(
                            script: "[ -f ${stateFile} ] && cat ${stateFile} || echo none",
                            returnStdout: true
                        ).trim()

                        echo """
🔍 ${app} (${branch})
   Latest : ${commit}
   Last   : ${lastBuilt}
"""

                        if (commit == lastBuilt) {
                            echo "⏭️ No changes → ${app}"
                            return
                        }

                        changesDetected = true

                        // 🔥 Parallel job trigger
                        jobs[app] = {

                            echo "🚀 Triggering ${app}"

                            gchatNotify("🚀 ${app} → dev")

                            build job: 'Build-Pipeline',
                                wait: false,
                                parameters: [
                                    string(name: 'APP_NAME', value: app),
                                    string(name: 'REPO_URL', value: repo),
                                    string(name: 'BRANCH_NAME', value: branch)
                                ]
                        }
                    }

                    if (!changesDetected) {
                        echo "⏭️ No changes across all repos"
                        currentBuild.description = "No changes"
                        return
                    }

                    if (jobs.isEmpty()) {
                        echo "⚠️ No jobs to run"
                        return
                    }

                    echo "🚀 Running parallel builds: ${jobs.keySet()}"
                    parallel jobs
                }
            }
        }
    }

    post {
        success {
            echo "✅ Orchestrator completed"
        }
        failure {
            echo "❌ Orchestrator failed"
        }
    }
}



// @Library('cicd-library') _

// pipeline {
//     agent any

//     triggers {
//         // 🔥 Use polling for now
//         cron('H/30 * * * *')
//     }

//     options {
//         disableConcurrentBuilds() // ✅ allow parallel triggers
//         buildDiscarder(logRotator(numToKeepStr: '5'))
//         timestamps()
//     }

//     environment {
//         CONTROL_REPO = "https://github.com/praveen-devops-labs/env-control-repo.git"
//     }

//     stages {

//         // 🔷 Start
//         stage('Start') {
//             steps {
//                 script {
//                     gchatNotify("🚀 Orchestrator started")
//                 }
//             }
//         }

//         // 🔷 Load Mapping
//         stage('Load Mapping') {
//             steps {
//                 script {
//                     dir('env-control') {
//                         deleteDir()

//                         git url: CONTROL_REPO,
//                             branch: 'main',
//                             credentialsId: 'github-token'
//                     }

//                     def config = readYaml file: 'env-control/mappings/current.yaml'

//                     if (!config?.mappings) {
//                         error "❌ mappings missing in YAML"
//                     }

//                     env.MAP = groovy.json.JsonOutput.toJson(config.mappings)

//                     echo "📄 Mapping loaded"
//                 }
//             }
//         }

//         // 🔷 Process (Polling)
//         stage('Process') {
//             steps {
//                 script {

//                     def parsed = readJSON text: env.MAP
//                     def jobs = [:]
//                     def changesDetected = false

//                     parsed.each { key, value ->

//                         def app     = value.app.toString()
//                         def repo    = value.repo.toString()
//                         def branch  = value.branch.toString()
//                         def envName = value.env.toString()

//                         // 🔥 Only dev builds
//                         if (envName != 'dev') {
//                             return
//                         }

//                         def commit

//                         withCredentials([usernamePassword(
//                             credentialsId: 'github-token',
//                             usernameVariable: 'GIT_USER',
//                             passwordVariable: 'GIT_PASS'
//                         )]) {

//                             commit = sh(
//                                 script: """
//                                 git ls-remote https://$GIT_USER:$GIT_PASS@${repo.replace('https://','')} refs/heads/${branch} | cut -f1
//                                 """,
//                                 returnStdout: true
//                             ).trim()
//                         }

//                         if (!commit) {
//                             echo "⚠️ Cannot fetch commit for ${app}"
//                             return
//                         }

//                         def stateFile = "/u01/jenkins/jenkins/build-state/${app}/dev.commit"

//                         def lastBuilt = sh(
//                             script: "[ -f ${stateFile} ] && cat ${stateFile} || echo none",
//                             returnStdout: true
//                         ).trim()

//                         echo "🔍 ${app} | ${branch}"
//                         echo "   Latest : ${commit}"
//                         echo "   Last   : ${lastBuilt}"

//                         if (commit == lastBuilt) {
//                             echo "⏭️ No changes → skipping ${app}"
//                             return
//                         }

//                         changesDetected = true

//                         jobs[key] = {

//                             echo "🚀 Triggering ${app} (${branch})"

//                             gchatNotify("🚀 ${app} | ${branch} → dev")

//                             build job: 'Build-Pipeline',
//                                 wait: false,
//                                 parameters: [
//                                     string(name: 'APP_NAME', value: app),
//                                     string(name: 'REPO_URL', value: repo),
//                                     string(name: 'BRANCH_NAME', value: branch)
//                                 ]
//                         }
//                     }

//                     if (!changesDetected) {
//                         echo "⏭️ No changes across all repos"
//                         currentBuild.description = "No changes"
//                         return
//                     }

//                     if (jobs.isEmpty()) {
//                         echo "⚠️ No jobs to run"
//                         return
//                     }

//                     parallel jobs
//                 }
//             }
//         }
//     }

//     post {
//         success {
//             echo "✅ Orchestrator completed"
//         }
//         failure {
//             echo "❌ Orchestrator failed"
//         }
//     }
// }