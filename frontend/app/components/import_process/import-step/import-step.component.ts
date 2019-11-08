import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';

@Component({
	selector: 'import-step',
	styleUrls: [ 'import-step.component.scss' ],
	templateUrl: 'import-step.component.html'
})
export class ImportStepComponent implements OnInit, OnChanges {

	public isCollapsed = false;
    @Input() selected_columns: any;
    @Input() added_columns: any;
    @Input() importId: any;
    importDataRes: any;
    validData: any;
    total_columns: any;
    csvResp: any;


	constructor(
        private stepService: StepsService, 
        private _ds: DataService,
        private toastr: ToastrService
        ) {}


	ngOnInit() {
	}


	onStepBack() {
		this.stepService.previousStep();
    }

    
    onImport() {
        this._ds.importData(this.importId, this.total_columns).subscribe(
            (res) => {		
                this.importDataRes = res;
                console.log(this.importDataRes);
            },
            (error) => {
                if (error.statusText === 'Unknown Error') {
                    // show error message if no connexion
                    this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                } else {
                    // show error message if other server error
                    console.log(error);
                    this.toastr.error(error.error.message + ' = ' + error.error.details);
                }
            }
        );

    }


    getValidData() {
        this._ds.getValidData(this.importId, this.selected_columns, this.added_columns).subscribe(
            (res) => {
                this.validData = res;
                this.total_columns = res['total_columns'];
                console.log(this.validData);
            },
            (error) => {
                if (error.statusText === 'Unknown Error') {
                    // show error message if no connexion
                    this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                } else {
                    // show error message if other server error
                    console.log(error);
                    this.toastr.error(error.error.message);
                }
            }
        );
    }


    onCSV() {
        this._ds.getCSV(this.importId).subscribe(
            (res) => {
                this.csvResp = res;
                console.log(this.csvResp);
            },
            (error) => {
                if (error.statusText === 'Unknown Error') {
                    // show error message if no connexion
                    this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                } else {
                    // show error message if other server error
                    console.log(error);
                    this.toastr.error(error.error.message);
                }
            }
        );
    }
    
}