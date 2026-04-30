@Library('cicd-library') _

pipeline {
    agent any

    parameters {
        string(name: 'APP_NAME', description: 'Application name')
        choice(name: 'TARGET_ENV', choices: ['dev','qa','uat','prod'], description: 'Target environment')
        string(name: 'VERSION', description: 'Docker image version')
        string(name: 'COMMIT_FULL', description: 'Full commit hash')
    }

    options {
        disableConcurrentBuilds()
        timestamps()
    }

    environment {
        IMAGE = "lavupraveen/${params.APP_NAME}:${params.VERSION}"
        STATE_DIR = "/u01/jenkins/jenkins/build-state"
    }

    stages {

        stage('Validate Input') {
            steps {
                script {
                    if (!params.APP_NAME || !params.VERSION || !params.COMMIT_FULL) {
                        error "❌ Missing required parameters"
                    }

                    echo """
                    🚀 Promotion Request
                    App     : ${params.APP_NAME}
                    Env     : ${params.TARGET_ENV}
                    Version : ${params.VERSION}
                    Commit  : ${params.COMMIT_FULL}
                    """
                }
            }
        }

        stage('Verify Image Exists') {
            steps {
                script {
                    def exists = sh(
                        script: "docker manifest inspect ${env.IMAGE} > /dev/null 2>&1 && echo yes || echo no",
                        returnStdout: true
                    ).trim()

                    if (exists != "yes") {
                        error "❌ Image not found: ${env.IMAGE}"
                    }

                    echo "✅ Image verified"
                }
            }
        }

        stage('Validate Promotion Flow') {
            steps {
                script {

                    // DEV → no validation
                    if (params.TARGET_ENV == 'dev') {
                        echo "⚡ DEV deployment → skipping flow validation"
                        return
                    }

                    def order = ['dev', 'qa', 'uat', 'prod']
                    def idx = order.indexOf(params.TARGET_ENV)

                    def prevEnv = order[idx - 1]

                    def prevFile = "${env.STATE_DIR}/${params.APP_NAME}/${prevEnv}.commit"

                    def prevCommit = sh(
                        script: "[ -f ${prevFile} ] && cat ${prevFile} || echo none",
                        returnStdout: true
                    ).trim()

                    if (prevCommit != params.COMMIT_FULL) {
                        error "❌ Cannot promote. ${prevEnv.toUpperCase()} not deployed for this commit"
                    }

                    echo "✅ Promotion flow valid"
                }
            }
        }

        stage('Approval for PROD') {
            when {
                expression { params.TARGET_ENV == 'prod' }
            }
            steps {
                input message: "🚨 Approve PROD deployment for ${params.APP_NAME} (${params.VERSION})?"
            }
        }

        stage('Deploy') {
            steps {
                script {

                    gchatNotify("🚀 PROMOTE ${params.APP_NAME} → ${params.TARGET_ENV}")

                    build job: 'Deploy-Pipeline',
                        parameters: [
                            string(name: 'APP_NAME', value: params.APP_NAME),
                            string(name: 'TARGET_ENV', value: params.TARGET_ENV),
                            string(name: 'COMMIT_FULL', value: params.COMMIT_FULL),
                            string(name: 'VERSION', value: params.VERSION)
                        ]
                }
            }
        }

        stage('Update State') {
            steps {
                script {
                    sh """
                    mkdir -p ${env.STATE_DIR}/${params.APP_NAME}
                    echo ${params.COMMIT_FULL} > ${env.STATE_DIR}/${params.APP_NAME}/${params.TARGET_ENV}.commit
                    """
                }
            }
        }
    }

    post {
        success {
            gchatNotify("✅ PROMOTION SUCCESS → ${params.APP_NAME} (${params.TARGET_ENV})")
        }
        failure {
            gchatNotify("❌ PROMOTION FAILED → ${params.APP_NAME} (${params.TARGET_ENV})")
        }
    }
}



// @Library('cicd-library') _

// pipeline {
//     agent any

//     parameters {
//         string(
//             name: 'APP_NAME',
//             description: 'Application name (e.g., app1, app2)'
//         )

//         choice(
//             name: 'TARGET_ENV',
//             choices: ['qa', 'uat', 'prod'],
//             description: 'Target environment for promotion'
//         )

//         string(
//             name: 'VERSION',
//             description: 'Docker image version (e.g., v1.0.74-4724ac4)'
//         )

//         string(
//             name: 'COMMIT_FULL',
//             description: 'Full Git commit hash (from Build-Pipeline)'
//         )
//     }

//     options {
//         disableConcurrentBuilds()
//         timestamps()
//     }

//     environment {
//         IMAGE = "lavupraveen/${params.APP_NAME}:${params.VERSION}"
//         STATE_DIR = "/u01/jenkins/jenkins/build-state"
//     }

//     stages {

//         stage('Validate') {
//             steps {
//                 script {

//                     if (!params.VERSION || !params.COMMIT_FULL) {
//                         error "❌ VERSION and COMMIT required"
//                     }

//                     echo """
//                     📦 Promotion Request
//                     App     : ${params.APP_NAME}
//                     Env     : ${params.TARGET_ENV}
//                     Version : ${params.VERSION}
//                     Commit  : ${params.COMMIT_FULL}
//                     """
//                 }
//             }
//         }

//         // 🔥 Ensure image exists
//         stage('Verify Image') {
//             steps {
//                 script {
//                     def exists = sh(
//                         script: "docker manifest inspect ${env.IMAGE} > /dev/null 2>&1 && echo yes || echo no",
//                         returnStdout: true
//                     ).trim()

//                     if (exists != "yes") {
//                         error "❌ Image not found: ${env.IMAGE}"
//                     }

//                     echo "✅ Image verified"
//                 }
//             }
//         }

//         // 🔥 Prevent invalid promotion flow
//         stage('Validate Flow') {
//             steps {
//                 script {

//                     def order = ['dev', 'qa', 'uat', 'prod']
//                     def targetIndex = order.indexOf(params.TARGET_ENV)

//                     def prevEnv = order[targetIndex - 1]

//                     if (prevEnv) {
//                         def prevFile = "${env.STATE_DIR}/${params.APP_NAME}/${prevEnv}.commit"

//                         def prevCommit = sh(
//                             script: "[ -f ${prevFile} ] && cat ${prevFile} || echo none",
//                             returnStdout: true
//                         ).trim()

//                         if (prevCommit != params.COMMIT_FULL) {
//                             error "❌ Cannot promote. ${prevEnv.toUpperCase()} not deployed for this commit"
//                         }
//                     }

//                     echo "✅ Promotion flow valid"
//                 }
//             }
//         }

//         // 🔥 Deploy
//         stage('Deploy') {
//             steps {
//                 script {

//                     gchatNotify("🚀 PROMOTE ${params.APP_NAME} → ${params.TARGET_ENV}")

//                     build job: 'Deploy-Pipeline',
//                         parameters: [
//                             string(name: 'APP_NAME', value: params.APP_NAME),
//                             string(name: 'TARGET_ENV', value: params.TARGET_ENV),
//                             string(name: 'COMMIT_FULL', value: params.COMMIT_FULL),
//                             string(name: 'VERSION', value: params.VERSION)
//                         ]
//                 }
//             }
//         }

//         // 🔥 Update state
//         stage('Update State') {
//             steps {
//                 script {
//                     sh """
//                     mkdir -p ${env.STATE_DIR}/${params.APP_NAME}
//                     echo ${params.COMMIT_FULL} > ${env.STATE_DIR}/${params.APP_NAME}/${params.TARGET_ENV}.commit
//                     """
//                 }
//             }
//         }
//     }

//     post {
//         success {
//             gchatNotify("✅ PROMOTION SUCCESS → ${params.APP_NAME} (${params.TARGET_ENV})")
//         }
//         failure {
//             gchatNotify("❌ PROMOTION FAILED → ${params.APP_NAME} (${params.TARGET_ENV})")
//         }
//     }
// }